from fastapi import FastAPI
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware

# Crear una instancia de la aplicación FastAPI
app = FastAPI()

# Definir la lista de orígenes permitidos
origins = ["*"]

# Habilitar CORS en la aplicación y permitir solicitudes desde cualquier origen
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Leer el archivo CSV con los datos de ventas de supermercado
df = pd.read_csv("supermarket_sales.csv")
df.head()

# Convertir la columna "Date" en formato de fecha y hora
df["Date"] = pd.to_datetime(df["Date"])
df.dtypes

# Extraer la hora de la columna "Time" y almacenarla en la columna "Hour"
df["Hour"] = pd.to_datetime(df["Time"]).dt.hour
df.dtypes

# Función asincrónica para realizar análisis univariable de los datos
async def univariate_data_analysis(groupby: str, interested_column: str, title: str):
    groupby_data = df.groupby(groupby)[interested_column].sum().sort_values(ascending=False)
    label_names = groupby_data.index.tolist()
    data = groupby_data.values.tolist()
    return_data = {"labels": label_names, "data": data, "label": title}
    return return_data

# Función asincrónica para obtener datos de las horas de compra
async def get_shopping_hour_data():
    bins = [0, 12, 18, 24]
    labels = ['Morning', 'Day time', 'Evening']
    df["Day time"] = pd.cut(x=df['Hour'], bins=bins, labels=labels, include_lowest=True)
    shopping_time = df.groupby("Branch")["Day time"].value_counts()

    final_shopping_time_data = {"labels": ["Branch A", "Branch B", "Branch C"], "label": "Shopping Hours By Branch"}

    for i, index in enumerate(["A", "B", "C"]):
        result = {"label": labels[i], "data": shopping_time[index].tolist()}
        final_shopping_time_data[index] = result

    return final_shopping_time_data

# Ruta principal de la aplicación
@app.get("/")
async def read_root():
    # Análisis de ventas por sucursal
    total_sales_per_branch = await univariate_data_analysis("Branch", "Total", "Total Sales Per Branch")

    # Análisis de ventas por género
    sales_by_gender = await univariate_data_analysis("Gender", "Total", "Sales By Gender")

    # Análisis de ingresos brutos por sucursal
    gross_income_data = await univariate_data_analysis("Branch", "gross income", "Gross Profit By Branch")

    # Análisis de líneas de productos por ventas totales
    product_line_by_total_sales = await univariate_data_analysis("Product line", "Total", "Product Line By Total Sales")

    # Análisis de líneas de productos por ingresos brutos
    product_line_by_gross_income = await univariate_data_analysis("Product line", "gross income", "Product Line By Gross Income")

    # Análisis de líneas de productos por calificación
    product_line_by_rating = await univariate_data_analysis("Product line", "Rating", "Product Line By Rating")

    # Análisis de métodos de pago por monto total de pago
    payment_methods = await univariate_data_analysis("Payment", "Total", "Payment Method By Total Payment")

    # Análisis de líneas de productos por cantidad
    product_line_by_quantity = await univariate_data_analysis("Product line", "Quantity", "Product Line By Quantity")

    # Obtener datos de las horas de compra
    shopping_hour_data = await get_shopping_hour_data()

    return {
        "data": {
            "total_sales_per_branch": total_sales_per_branch,
            "sales_by_gender": sales_by_gender,
            "gross_income_data": gross_income_data,
            "product_line_by_total_sales": product_line_by_total_sales,
            "product_line_by_gross_income": product_line_by_gross_income,
            "product_line_by_rating": product_line_by_rating,
            "payment_methods": payment_methods,
            "product_line_by_quantity": product_line_by_quantity,
            "shopping_hour_data": shopping_hour_data
        }
    }

# En resumen, esta aplicación FastAPI realiza análisis univariados de datos de ventas de supermercado. Algunas de las funcionalidades incluyen:

# Habilitar el uso de CORS para permitir solicitudes desde cualquier origen.
# Leer un archivo CSV con los datos de ventas de supermercado.
# Convertir la columna de fechas en formato de fecha y hora.
# Extraer la hora de la columna de tiempo y almacenarla en una nueva columna.
# Definir funciones asincrónicas para realizar análisis univariados de los datos.
# Definir una función asincrónica para obtener datos sobre las horas de compra en cada sucursal.
# Configurar una ruta principal que ejecuta los análisis y devuelve los resultados en un formato JSON estructurado.