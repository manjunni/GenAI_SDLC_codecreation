from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import AzureChatOpenAI
import os
from pydantic import BaseModel, Field
#from langchain_core.caches import BaseCache
#import unstructured
#from unstructured.partition.xlsx import partition_xlsx
#import networkx as nx
from langgraph.graph import StateGraph,START,END
from dotenv import load_dotenv
from langsmith import utils
#modules for tracing
from azure.ai.projects import AIProjectClient,enable_telemetry
from azure.identity import DefaultAzureCredential
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry.instrumentation.langchain import LangchainInstrumentor
from RAG_azure import search_vectorstore
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs,compute
import tempfile

load_dotenv(
    dotenv_path='.env.local',override=True
)
utils.tracing_is_enabled()
enable_telemetry()
azure_endpoint=os.getenv("OPENAI_API_ENDPOINT")
os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"] = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
configure_azure_monitor()
project_client= AIProjectClient(endpoint=azure_endpoint, credential=DefaultAzureCredential(exclude_interactive_browser_credential=True))
with project_client.inference.get_azure_openai_client(api_version=os.getenv("OPENAI_API_VERSION")) as client:
     
    instrumentator = LangchainInstrumentor()
    if not instrumentator.is_instrumented_by_opentelemetry:
        instrumentator.instrument()

    #def create_pipeline_code(docs,domain_doc,dataengg_doc,use_domain_doc):
    def create_pipeline_code(docs,use_domain_doc,max_attempts,source_path,target_path,target_language="python"):
               
        #model
        llm = AzureChatOpenAI(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        azure_endpoint=os.getenv("OPENAI_API_ENDPOINT"),
        openai_api_version=os.getenv("OPENAI_API_VERSION"), 
        deployment_name=os.getenv("DEPLOYMENT_NAME"),
        request_timeout=600,  # Set timeout to 10 minutes        
        )   
        
        # parse_prompt = ChatPromptTemplate.from_messages(
        #     [("system", "Based on the {context},generate a dataflow for giving as input to code generation agent identify source and target tables")],
        # )    
        # parse_chain = parse_prompt | llm 
        # dataflow=parse_chain.invoke({"context": docs})

        #Generate code
        class Code(BaseModel):
            comments: str = Field(description="Initial comments")
            #imports: str = Field(description="import statements")
            code: str = Field(description="Python code without imports")
        class CodeRefactor(BaseModel):
            common_functions: str = Field(description="Common functions extracted from the code")
            connection_details: str = Field(description="Connection details extracted from the code")

        ###Prompts############    
        if use_domain_doc:
            vectorsearch_keywords = "data description, transformation logic"
            data_description = search_vectorstore(vectorsearch_keywords)
            data_description_str = ''.join([result.page_content for result in data_description])           
            Codegen_prompt = ChatPromptTemplate.from_messages( 
                [
                    (
                        "system", 
                        "Based on the dataflow and transformation logic in {mapping_file},generate {target_language} code for data pipeline that can be run on databricks.Ingest data from {source_path} and write output table to {target_path}.structure the output as a comments and code block."
                    ),
                    (
                        "placeholder","{messages}"
                    )
                    ])
        else:
            data_description_str = ""
            Codegen_prompt = ChatPromptTemplate.from_messages( 
                [
                    (
                        "system", 
                        "Based on the dataflow and transformation logic in {mapping_file},generate {target_language} code for data pipeline that can be run on databricks.Ingest data from {source_path} and write output table to {target_path}.structure the output as a comments and code block"
                    ),
                    (
                        "placeholder","{messages}"
                    )
                    ])
     
        Coderefactor_prompt = ChatPromptTemplate.from_messages( 
            [
                (
                    "system", 
                    "Refactor the {code_solution} to extract common functions and data connections.structure the output as a common function and connection details."
                )
                ])
        Debug_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system", 
                    "Reflect on the error {messages} and fix {code}."
                )
            ]
        )
        #####chains###########
        Codegen_chain = Codegen_prompt |llm.with_structured_output(Code)
        Coderefactor_chain = Coderefactor_prompt | llm.with_structured_output(CodeRefactor)
        Debug_chain = Debug_prompt | llm.with_structured_output(Code)
        # Initialize messages as an empty list
        messages = []

        #State
        from typing import List
        from typing_extensions import TypedDict

        class GraphState(TypedDict):
            error: str
            messages: List
            generation:str
            iteration:int
            common_functions: str
            connection_details: str

        ##Graph
        ###Parameters
        max_iterations = max_attempts
        #Reflect
        #flag = 'reflect'
        #flag = 'do not reflect'

        ###Nodes
        def generate_code(state: GraphState):
            """
            Generate code based on the current state of the graph.
            Args:
                state(dict):current state of the graph
            Returns:
                state(dict):new key added to state, generation"""
            # Extract the error message and other relevant information from the state
            # error = state["error"]
            # messages = state["messages"]
            # code_solution = state["generation"]
            iteration = state["iteration"]


            # if error == "yes":
            #     messages += [
            #         (
            #             "user",
            #             "Now, try again. Invoke the code tool to structure the output with a prefix, imports, and code block:",
            #         )
            #     ]
            code_solution = Codegen_chain.invoke(
                {"mapping_file": docs,"target_language": target_language,"source_path":source_path, "target_path":target_path}
            )
            iteration += 1
            return {"generation": code_solution,"iteration": iteration}
        def refactor_code(state: GraphState):
            """
            Generate code based on the current state of the graph.
            Args:
                state(dict):current state of the graph
            Returns:
                state(dict):new key added to state, generation"""
            # Extract the error message and other relevant information from the state
            error = state["error"]
            messages = state["messages"]
            code_solution = state["generation"]
            iteration = state["iteration"]
            
            code_refactored = Coderefactor_chain.invoke(
                {"code_solution": code_solution.code}
            )
            
            return {"generation": code_solution,
                    "common_functions": code_refactored.common_functions, 
                    "connection_details": code_refactored.connection_details,
                    "error": error,
                    "messages": messages}

        def code_check(state: GraphState):
            """
            check code

            Args:
                state(dict):current state of the graph
            Returns:
                state(dict):new key added to state, error
            """
            messages = state["messages"]
            code_solution = state["generation"]
            iteration = state["iteration"]

            #imports = code_solution.imports
            code = code_solution.code
            DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
            DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
            DATABRICKS_CLUSTER_ID=os.getenv("DATABRICKS_CLUSTER_ID")
            print("Executing code...")
            with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
                tmp.write(code)
                local_path = tmp.name

            dbfs_path = f"/tmp/{os.path.basename(local_path)}"
            try:
                w = WorkspaceClient(
                    host=DATABRICKS_HOST,
                    token=DATABRICKS_TOKEN,
                    cluster_id=DATABRICKS_CLUSTER_ID
                )
                with open(local_path, "rb") as f:
                    w.dbfs.upload(dbfs_path, f, overwrite=True)
                    # Submit a one-time job to run the PySpark code on the cluster
                
                run = w.jobs.submit_and_wait(
                    run_name=f"LangChain Code Execution Test",
                    tasks=[
                        jobs.SubmitTask(
                            task_key="execute_code",
                            # new_cluster=compute.ClusterSpec(
                            #     spark_version="13.3.x-scala2.12",#w.clusters.select_spark_version(long_term_support=True),
                            #     node_type_id="Standard_DS3_v2",#w.clusters.select_node_type(local_disk=True),
                            #     num_workers=1,
                            # ),
                            existing_cluster_id=DATABRICKS_CLUSTER_ID,
                            spark_python_task=jobs.SparkPythonTask(python_file=f"dbfs:{dbfs_path}")
                        )
                    ],
                )
                # Wait for job to finish and check result
                run_id = run.run_id
                run = w.jobs.get_run(run_id)
                task_run_ids = [task.run_id for task in run.tasks]
                # print(task_run_ids)
                # For a single-task job, you can access the first task's run ID
                first_task_run_id = run.tasks[0].run_id
                print(first_task_run_id) 
                output=w.jobs.get_run_output(first_task_run_id)
                # print(run)
                if output.metadata.state.result_state.value == "SUCCESS":
                    print("---No code errors---")
                    error = "no"
                else:
                    print(output.error)  
                    print(output.error_trace) 
                return {
                        "generation": code_solution,
                         "messages": messages,
                         "error": error,
                         "iteration": iteration,
                    }

            except Exception as e:
                print(f"Error executing code: {e}")
                runs = list(w.jobs.list_runs())  
                latest_run_id = runs[0].run_id if runs else None
                run = w.jobs.get_run(latest_run_id)      
                first_task_run_id = run.tasks[0].run_id  
                output=w.jobs.get_run_output(first_task_run_id)                
                error_message = [("user",f"your solution failed the code execution test:{output.error} \n {output.error_trace}")]
                messages += error_message
                error = "yes"
                return{
                                "generation": code_solution,
                                "messages": messages,
                                "error": error,
                                "iteration": iteration,     
                            }
            #if no error, return the code
            print("---No code errors---")
            return {
                "generation": code_solution,
                "messages": messages,
                "error": "no",
                "iteration": iteration,
            }
        def debug(state: GraphState):
            """
            Reflect on the error and provide feedback.

            Args:
                state(dict):current state of the graph
            Returns:
                state(dict):new key added to state, reflections
            """
            messages = state["messages"]
            code_solution = state["generation"]
            iteration = state["iteration"]

            #Debug the code
            fixed_code =Debug_chain.invoke(
                {"messages": messages,"code": code_solution.code}
            )
            # messages += [
            #     (
            #         "assistant",
            #         f"Here are the reflections on the error: {fixed_code.code}",
            #     )
            # ]
            iteration += 1
            return {
                "generation": fixed_code,
                "messages": messages,
                "iteration": iteration,
            }


        def decision(state: GraphState):
            """
            Make a decision to finish based on the current state of the graph.

            Args:
                state(dict):current state of the graph
            Returns:
                str: next node to call
            """
            error = state["error"]
            iteration = state["iteration"]

            #Make a decision based on the error
            if error == "no" or iteration == max_iterations:
                print("Finished")
                return "end"
            else:
                return "debug"
                # print("Not finished")
                # if flag == "reflect":
                #     return "reflect"
                # else:
                #     return "generate_code"
        ##Workflow

        workflow = StateGraph(GraphState)

        #Define the nodes
        workflow.add_node("generate_code", generate_code)
        workflow.add_node("code_check", code_check)
        workflow.add_node("debug", debug)
        workflow.add_node("refactor_code", refactor_code)   

        #Build the graph
        workflow.add_edge(START, "generate_code")
        #workflow.add_edge("generate_code", "refactor_code")
        #workflow.add_edge("refactor_code", "code_check")
        workflow.add_edge("generate_code", "code_check")     
        workflow.add_conditional_edges(
            "code_check",
            decision,
            {
                "end": "refactor_code",
                "debug": "debug",
                
            },
        )
        workflow.add_edge("debug", "code_check")
        workflow.add_edge("refactor_code", END)
        app = workflow.compile()
        #Run the graph 
        solution = app.invoke(
            
            {
                "error": "no",
                "messages": [],
                "generation": None,
                "iteration": 0,
            }
        )


        return solution

