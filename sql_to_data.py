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

        print(f"Command Status for Command ID {command_id}:")
        #print(json.dumps(status, indent=2))

        if results:
            #print("\nResults:")
            #print(json.dumps(results, indent=2))
            # Extract column names from the schema
            column_names = [column['name'] for column in results['schema']]
            # Extract data values from the 'data' field
            data_values = results['data']
            # Create a Pandas DataFrame
            df = pd.DataFrame(data_values, columns=column_names)
            return(df)
        else:
            print("\nResults not available yet. The command may still be running.")
    else:
        print(f"Error: {response.status_code}, {response.text}")



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