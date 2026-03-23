import streamlit as st
from snowflake.snowpark.functions import col
import pandas as pd   # ✅ REQUIRED
import requests  

st.title("Customize ur smoothie")

st.write("""Choose the fruits you want in your custom smoothie""")

name_on_order = st.text_input('Name of smoothie: ')
st.write('The name on your smoothie will be ', name_on_order)

cnx = st.connection("snowflake")
session = cnx.session()

# ✅ include SEARCH_ON
my_dataframe = session.table("smoothies.public.fruit_options") \
    .select(col('FRUIT_NAME'), col('SEARCH_ON'))

# ✅ convert to pandas
pd_df = my_dataframe.to_pandas()

#st.dataframe(pd_df)

# ❌ REMOVE this line (wrong)
# fruit_map = dict(zip(my_dataframe['FRUIT_NAME'], my_dataframe['SEARCH_ON']))

# ✅ fix multiselect
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients: ',
    pd_df['FRUIT_NAME'],
    max_selections=5
)

if ingredients_list: 
    ingredients_string = ''

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '

        # ✅ correct way (lab method)
        search_value = pd_df.loc[
            pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'
        ].iloc[0]

        st.write('The search value for ', fruit_chosen, ' is ', search_value, '.')

        st.subheader(fruit_chosen + ' Nutrition Information')

        smoothiefroot_response = requests.get(
            f"https://my.smoothiefroot.com/api/fruit/{search_value}"
        )

        st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    st.write(my_insert_stmt)
    time_to_insert = st.button('Submit Order')
    
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!', icon="✅")
