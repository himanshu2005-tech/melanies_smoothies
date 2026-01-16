# Import python packages
import streamlit as st
import pandas as pd
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
st.write("Choose your fruits and we'll fetch nutrition information!")

# ----------------------------------
# CUSTOMER ENTERS NAME
# ----------------------------------
name_on_order = st.text_input("Name on smoothie:")
st.write("The name on your smoothie will be:", name_on_order)

# ----------------------------------
# LOAD FRUIT OPTIONS
# ----------------------------------
my_dataframe = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS") \
    .select(col("FRUIT_NAME"), col("SEARCH_ON"))

# Convert Snowpark → Pandas so we can use LOC
pd_df = my_dataframe.to_pandas()

# ----------------------------------
# USER SELECTS INGREDIENTS
# ----------------------------------
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    pd_df["FRUIT_NAME"],
    max_selections=5
)

# ----------------------------------
# NUTRITION LOOKUP USING SEARCH_ON
# ----------------------------------
if ingredients_list:

    ingredients_string = ""

    for fruit_chosen in ingredients_list:

        # Build final string for order insert
        ingredients_string += fruit_chosen + " "

        # Lookup the SEARCH_ON value
        search_on = pd_df.loc[
            pd_df["FRUIT_NAME"] == fruit_chosen, "SEARCH_ON"
        ].iloc[0]

        # Debug sentence (should look correct)
        st.write(f"The search value for {fruit_chosen} is {search_on}.")

        # ----------------------------------
        # API CALL USING SEARCH_ON VALUE
        # ----------------------------------
        st.subheader(fruit_chosen + " Nutrition Information")

        smoothiefroot_response = requests.get(
            f"https://my.smoothiefroot.com/api/fruit/{search_on}"
        )

        # Show nutrition response
        st.dataframe(
            smoothiefroot_response.json(),
            use_container_width=True
        )

    # ----------------------------------
    # INSERT ORDER INTO SNOWFLAKE
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
