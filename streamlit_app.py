# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched
import requests

# Connect to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()
role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

st.write("🔎 Streamlit App is running under role:", role)

st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
    """
    Select your ingredients and place your smoothie order!
    """
)

# Customer enters name
name_on_order = st.text_input("Name on smoothie: ")
st.write("The name on smoothie will be ", name_on_order)

# Load fruit options
fruit_df = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS").select(col('FRUIT_NAME'))

# Select ingredients
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    fruit_df,
    max_selections=5
)

# -----------------------------------------------------
#   INGREDIENTS + NUTRITION INFO (API CALL IN LOOP)
# -----------------------------------------------------
if ingredients_list:

    ingredients_string = ""

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + " "

        # Show nutrition header
        st.subheader(fruit_chosen + " Nutrition Information")

        # API CALL
        smoothiefroot_response = requests.get(
            "https://my.smoothiefroot.com/api/fruit/" + fruit_chosen
        )

        # Show nutrition data
        sf_df = st.dataframe(
            data=smoothiefroot_response.json(),
            use_container_width=True
        )

    # Insert order SQL
    my_insert_stmt = f"""
        INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    if st.button('Submit Order'):
        session.sql(my_insert_stmt).collect()
        st.success("Your Smoothie is ordered!", icon="✅")

# ----------------------------
#   PENDING ORDERS (EDITABLE)
# ----------------------------
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

    # ----------------------
    # MERGE STATEMENT
    # ----------------------
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
