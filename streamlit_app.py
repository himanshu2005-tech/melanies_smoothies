# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched
import requests

# Connect to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()
role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

st.write("🔎 Streamlit App is running under role:", role)

st.title("🥤 Customize Your Smoothie! 🥤")
st.write("Choose your ingredients and we’ll fetch nutrition info!")

# -------------------------
# CUSTOMER ENTERS NAME
# -------------------------
name_on_order = st.text_input("Name on smoothie: ")
st.write("The name on your smoothie will be:", name_on_order)

# -------------------------
# LOAD FRUIT OPTIONS
# -------------------------
my_dataframe = session.table("SMOOTHIES.PUBLIC.FRUIT_OPTIONS") \
    .select(col("FRUIT_NAME"), col("SEARCH_ON"))

# Show the dataframe during debugging
st.dataframe(my_dataframe, use_container_width=True)

# TEMP STOP (like instructions show)
# Remove this after testing!
# st.stop()

# -------------------------
# SELECT FRUITS FOR SMOOTHIE
# -------------------------
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    my_dataframe["FRUIT_NAME"],
    max_selections=5
)

# -------------------------------------------
# SHOW NUTRITION FOR EACH FRUIT SELECTED
# -------------------------------------------
if ingredients_list:

    # Dictionary: Fruit Name → SEARCH_ON
    fruit_lookup = dict(zip(my_dataframe["FRUIT_NAME"], my_dataframe["SEARCH_ON"]))

    ingredients_string = ""

    for fruit_chosen in ingredients_list:

        search_term = fruit_lookup[fruit_chosen]  # <= correct mapping!
        ingredients_string += fruit_chosen + " "

        # Show title
        st.subheader(f"{fruit_chosen} Nutrition Information")

        # API CALL
        response = requests.get(
            "https://my.smoothiefroot.com/api/fruit/" + search_term
        )

        # Display nutrition
        st.dataframe(response.json(), use_container_width=True)

    # ------------------------------------
    # INSERT ORDER INTO SNOWFLAKE
    # ------------------------------------
    insert_sql = f"""
        INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER)
        VALUES ('{ingredients_string}', '{name_on_order}')
    """

    if st.button("Submit Order"):
        session.sql(insert_sql).collect()
        st.success("Your Smoothie is ordered!", icon="✅")

# ----------------------------
#   PENDING ORDERS SECTION
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
