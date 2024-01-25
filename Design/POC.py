from openai import OpenAI
client = OpenAI(api_key="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")

while True:
    query = input("User: ")
    if "quit" in query:
        break
    completion = client.chat.completions.create(
    model="gpt-3.5-turbo",
    messages=[{"role": "system", "content": """You are SQL Expert, based on the database schema info, attributes calculation and information from user generate a sql query.
          Don't give any explanation unless asked, just output the query. Ask if theres any doubt, don't output wrong info. 
                
         Database schema: 
         Table 1. table_name = users, columns = [slab_name, user_id, mobile], context = Each row is information of distinct user.
         Table 2. table_name = bill_summary, columns = [bill_id, dim_event_user_id, bill_amount, dim_event_zone_till_id, dim_event_date_id (this is just a id not a date, but a reference to date table)], context = Each row is information of about a bill.. one user can make
           multiple bills.avg sak
        Table 3 table_name = zone_tills, columns = [external_id_2, store_name, state, city, zone_till_id], context= each row is store level info.
        Table 4 table_name = date, columns = [date_id, date, year] context=each row is each date's info.
        
        Attributes: context= Attributes are additional segmentation of a user based on the given definition given. (calculate using the given formuales using the most appropriate columns from the database schema)
        1. users attributes: 
        explanation/calculation of attributes
          1. new_existing_tag = segment a user as a new or existing user.calculate if min(bill_date) is greater or equal to current financial year's start date (not a predefined column) then tag it as 'new' else 'existing'
          
          
          """},
        {"role": "user", "content": f"{query}"}])
    print(completion.choices[0].message.content)
    print("\n")
