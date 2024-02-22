import requests
import json
import pandas as pd
import time
import streamlit as st

def execution_context_creation():
    
    workspace_url = "https://capillary-notebook-sgcrm.cloud.databricks.com"
    api_token = st.secrets.databricks.api_token
    cluster_id= st.secrets.databricks.cluster_id

    # Databricks REST API endpoint for creating an execution context
    endpoint = f"{workspace_url}/api/1.2/contexts/create"

    # Request headers
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # Request payload
    payload = {
        "clusterId": cluster_id,  # Replace with your running cluster id
        "language": "sql"
    }

    # Send the POST request to create an execution context
    response = requests.post(endpoint, headers=headers, data=json.dumps(payload))

    # Check the response
    if response.status_code == 200:
        result = response.json()
        context_id = result.get('id')
        print(f"Execution context created successfully. Context ID: {context_id}")
    else:
        print(f"Error: {response.status_code}, {response.text}")
    return(context_id)



def sql_execution(sql_query, context_id, cluster_id):
    # Databricks REST API endpoint for running a cluster command
    workspace_url = "https://capillary-notebook-sgcrm.cloud.databricks.com"
    api_token = st.secrets.databricks.api_token
    cluster_id= st.secrets.databricks.cluster_id

    endpoint = f"{workspace_url}/api/1.2/commands/execute"

    # Request headers
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # Request payload
    payload = {
        "clusterId": cluster_id,  # Replace with your running cluster id
        "contextId": context_id,  # Replace with your running context id
        "language": "sql",  # Choose python, scala, or sql
        "command": sql_query,  # Replace with your executable code
    }

    # Send the request to run the cluster command
    response = requests.post(endpoint, headers=headers, data=json.dumps(payload))

    # Check the response
    if response.status_code == 200:
        result = response.json()
        command_id = result.get('id')
        print(f"Command submitted successfully. Command ID: {command_id}")
    else:
        print(f"Error: {response.status_code}, {response.text}")
        
    return(command_id)



def data_retrieval(context_id, command_id,cluster_id):

    # Databricks REST API endpoint for getting command status
    workspace_url = "https://capillary-notebook-sgcrm.cloud.databricks.com"
    api_token = st.secrets.databricks.api_token
    cluster_id= st.secrets.databricks.cluster_id
    endpoint = f"{workspace_url}/api/1.2/commands/status"

    # Request headers
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # Request parameters
    params = {
        "clusterId": cluster_id,
        "contextId": context_id,
        "commandId": command_id,
    }

    # Send the GET request to get command status
    response = requests.get(endpoint, headers=headers, params=params)

    # Check the response
    if response.status_code == 200:
        result = response.json()
        status = result.get('status', {})
        results = result.get('results', {})
        
        # Convert to JSON string with indentation
        json_view = json.dumps(result, indent=4, sort_keys=True)

        # Print the formatted JSON
        print("Result JSON: \n", json_view)

        if response.status_code == 200:
            result = response.json()

            if results:
                if results["resultType"] == 'error':
                    return {"success": False, "message": "Error in code"}  # Return a specific value to signal error

                # Create DataFrame
                column_names = [column['name'] for column in results['schema']]
                data_values = results['data']
                df = pd.DataFrame(data_values, columns=column_names)
                return {"success": True, "data": df}  # Return the DataFrame directly

            else:
                print("Results not available yet. The command may still be running.")
                return None  #  Return None to indicate no DataFrame yet

        else:
            print(f"Error: {response.status_code}, {response.text}")
            return None



def trigger_retrieval_loop(context_id, command_id, cluster_id):
    api_token = st.secrets.databricks.api_token
    cluster_id = st.secrets.databricks.cluster_id
    timeout_seconds = 300  # Timeout after 5 minutes (5 * 60 seconds)
    start_time = time.time()
    max_iterations = 10  # Temporary safety limit

    iteration_count = 0
    while True:
        try:
            print("Attempting Data Retrieval")
            result = data_retrieval(context_id, command_id, cluster_id)  

            if not result["success"]:
                print(f"Error encountered: {result['message']}")
                return None 
        
            elif result is not None:  
                df = result["data"]
                return df 

        except Exception as e:
            print(f"Error during data retrieval: {e}")

        time.sleep(2)  # Wait for 2 seconds

        elapsed_time = time.time() - start_time
        if elapsed_time > timeout_seconds:
            print("Didn't complete in time")
            return None

        iteration_count += 1
        if iteration_count >= max_iterations:
            print("Reached maximum iterations. Potential issue with retrieval.")
            return None




def destroy_execution_context(cluster_id, context_id):
    # Databricks REST API endpoint for deleting an execution context
    workspace_url = "https://capillary-notebook-sgcrm.cloud.databricks.com"
    api_token = st.secrets.databricks.api_token
    cluster_id= st.secrets.databricks.cluster_id
    endpoint = f"{workspace_url}/api/1.2/contexts/destroy"

    # Request headers
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # Request payload
    payload = {
        "clusterId": cluster_id,        # Replace with your cluster id
        "contextId": context_id    # Replace with your context id
    }

    # Send the request to delete the execution context
    response = requests.post(endpoint, headers=headers, data=json.dumps(payload))

    # Check the response
    if response.status_code == 200:
        print("******Execution context deleted successfully.******")
    else:
        print(f"Error: {response.status_code}, {response.text}")