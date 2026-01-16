# Import python packages
import streamlit as st
import pandas as pd            # NEW – Pandas for loc/iloc
from snowflake.snowpark.functions import col, when_matched
import requests

# ----------------------------------
# CONNECT TO SNOWFLAKE
# ----------------------------------
cnx = st.connection("snowflake")
session = cnx.session()
role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

st.write("🔎 Streamlit App is running under role:", role)

st.title("🥤 Customize Your Smoothie! 🥤")
st.write("Choose your fruits and view nutrition information!")

# ----------------------------------
# CUSTOMER ENTERS NAME
# ----------------------------------
name_on_order = st.text_input("Name on smoothie:")
st.write("The name on your smoothie will be:", name_on_order)

# ----------------------------------
# LOAD FRUITS (FRUIT_NAME + SEARCH_ON)
# ----------------------------------
my_dataframe = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS") \
    .select(col("FRUIT_NAME"), col("SEARCH_ON"))

# Show Snowpark DF for debugging
# st.dataframe(my_dataframe)

# Convert Snowpark DF → Pandas DF
pd_df = my_dataframe.to_pandas()

# Show Pandas DF for debugging
# st.dataframe(pd_df)
# st.stop()

# ----------------------------------
# USER SELECTS INGREDIENTS
# ----------------------------------
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    pd_df["FRUIT_NAME"],
    max_selections=5
)

# ----------------------------------
# SHOW NUTRITION + SEARCH_ON LOOKUP
# ----------------------------------
if ingredients_list:

    ingredients_string = ""

    for fruit_chosen in ingredients_list:

        # Build ingredient list string
        ingredients_string += fruit_chosen + " "

        # ----------------------------------
        # Find SEARCH_ON using Pandas LOC
        # ----------------------------------
        search_on = pd_df.loc[
            pd_df["FRUIT_NAME"] == fruit_chosen, "SEARCH_ON"
        ].iloc[0]

        # Debug line (should show correct mapping)
        st.write("The search value for", fruit_chosen, "is", search_on, ".")

        # ----------------------------------
        # CALL THE API USING SEARCH_ON
        # ----------------------------------
        st.subheader(fruit_chosen + " Nutrition Information")

        fruityvice_response = requests.get(
            "https://my.smoothiefroot.com/api/fruit/" + search_on
        )

        # Show the nutrition results
        st.dataframe(
            fruityvice_response.json(),
            use_container_width=True
        )

    # ----------------------------------
    # INSERT ORDER
    # ----------------------------------
    insert_sql = f"""
        INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    if st.button("Submit Order"):
        session.sql(insert_sql).collect()
        st.success("Your Smoothie is ordered!", icon="✅")

# ----------------------------------
# PENDING ORDERS (EDITABLE)
# ----------------------------------
st.header("🧋 Pending Smoothie Orders!")
st.write("Tick the checkbox to mark an order as filled.")

try:
    orders_df = (
        session.table("SMOOTHIES.PUBLIC.ORDERS")
        .filter(col("ORDER_FILLED") == 0)
        .select(
            col("ORDER_UID"),
            col("INGREDIENTS"),
            col("NAME_ON_ORDER"),
            col("ORDER_FILLED")
        )
        .collect()
    )

    editable_df = st.data_editor(orders_df, key="orders_editor")

    # ----------------------------------
    # MERGE TO UPDATE ORDER_FILLED
    # ----------------------------------
    if st.button("Submit"):
        og_dataset = session.table("SMOOTHIES.PUBLIC.ORDERS")
        edited_dataset = session.create_dataframe(editable_df)

        og_dataset.merge(
            edited_dataset,
            (og_dataset["ORDER_UID"] == edited_dataset["ORDER_UID"]),
            [
                when_matched().update({
                    "ORDER_FILLED": edited_dataset["ORDER_FILLED"]
                })
            ]
        )

        st.success("Order updates saved!", icon="🥋")
        st.experimental_rerun()

except Exception as e:
    st.error("Orders table unavailable or insufficient privileges.", icon="❌")
    st.code(str(e))
