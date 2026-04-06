from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from google.cloud import bigquery

# Your specific Google Cloud Project ID
PROJECT_ID = "starry-sunup-489716-a3" 
DATASET = f"{PROJECT_ID}.property_mgmt"

app = FastAPI(title="Property Management API")

# Enable CORS for frontend connection
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = bigquery.Client(project=PROJECT_ID)

# --- Pydantic Models for our Data ---
class PropertyCreate(BaseModel):
    property_name: str
    address: str
    city: str
    state: str
    zip_code: str
    monthly_rent: float
    tenant_name: Optional[str] = None

class IncomeCreate(BaseModel):
    amount: float
    date_received: str
    payment_type: Optional[str] = None
    notes: Optional[str] = None

class ExpenseCreate(BaseModel):
    amount: float
    expense_date: str
    category: Optional[str] = None
    notes: Optional[str] = None

# --- REQUIRED ENDPOINTS (IA 9) ---

@app.get("/properties")
def get_all_properties():
    query = f"SELECT * FROM `{DATASET}.properties`"
    query_job = client.query(query)
    results = [dict(row) for row in query_job]
    return {"properties": results}

@app.get("/properties/{property_id}")
def get_single_property(property_id: int):
    query = f"SELECT * FROM `{DATASET}.properties` WHERE property_id = @prop_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("prop_id", "INT64", property_id)])
    query_job = client.query(query, job_config=job_config)
    results = [dict(row) for row in query_job]
    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    return results[0]

@app.get("/income/{property_id}")
def get_property_income(property_id: int):
    query = f"SELECT * FROM `{DATASET}.income` WHERE property_id = @prop_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("prop_id", "INT64", property_id)])
    query_job = client.query(query, job_config=job_config)
    return {"income_records": [dict(row) for row in query_job]}

@app.post("/income/{property_id}")
def create_income(property_id: int, income: IncomeCreate):
    id_query = f"SELECT COALESCE(MAX(income_id), 0) + 1 as next_id FROM `{DATASET}.income`"
    next_id = list(client.query(id_query))[0].next_id

    query = f"""
        INSERT INTO `{DATASET}.income` (income_id, property_id, amount, date, description)
        VALUES (@inc_id, @prop_id, @amount, @date, @notes)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("inc_id", "INT64", next_id),
            bigquery.ScalarQueryParameter("prop_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", income.amount),
            bigquery.ScalarQueryParameter("date", "DATE", income.date_received),
            bigquery.ScalarQueryParameter("notes", "STRING", income.notes),
        ]
    )
    client.query(query, job_config=job_config).result()
    return {"message": "Income record created successfully", "income_id": next_id}

@app.get("/expenses/{property_id}")
def get_property_expenses(property_id: int):
    query = f"SELECT * FROM `{DATASET}.expenses` WHERE property_id = @prop_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("prop_id", "INT64", property_id)])
    query_job = client.query(query, job_config=job_config)
    return {"expense_records": [dict(row) for row in query_job]}

@app.post("/expenses/{property_id}")
def create_expense(property_id: int, expense: ExpenseCreate):
    id_query = f"SELECT COALESCE(MAX(expense_id), 0) + 1 as next_id FROM `{DATASET}.expenses`"
    next_id = list(client.query(id_query))[0].next_id

    query = f"""
        INSERT INTO `{DATASET}.expenses` (expense_id, property_id, amount, date, category, description)
        VALUES (@exp_id, @prop_id, @amount, @date, @cat, @notes)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("exp_id", "INT64", next_id),
            bigquery.ScalarQueryParameter("prop_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", expense.amount),
            bigquery.ScalarQueryParameter("date", "DATE", expense.expense_date),
            bigquery.ScalarQueryParameter("cat", "STRING", expense.category),
            bigquery.ScalarQueryParameter("notes", "STRING", expense.notes),
        ]
    )
    client.query(query, job_config=job_config).result()
    return {"message": "Expense record created successfully", "expense_id": next_id}

# --- 4 CUSTOM ADDITIONAL ENDPOINTS ---

@app.post("/properties")
def create_property(prop: PropertyCreate):
    id_query = f"SELECT COALESCE(MAX(property_id), 0) + 1 as next_id FROM `{DATASET}.properties`"
    next_id = list(client.query(id_query))[0].next_id

    query = f"""
        INSERT INTO `{DATASET}.properties` (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
        VALUES (@p_id, @p_name, @addr, @city, @state, @zip, 'Standard', @tenant, @rent)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_id", "INT64", next_id),
            bigquery.ScalarQueryParameter("p_name", "STRING", prop.property_name),
            bigquery.ScalarQueryParameter("addr", "STRING", prop.address),
            bigquery.ScalarQueryParameter("city", "STRING", prop.city),
            bigquery.ScalarQueryParameter("state", "STRING", prop.state),
            bigquery.ScalarQueryParameter("zip", "STRING", prop.zip_code),
            bigquery.ScalarQueryParameter("rent", "FLOAT64", prop.monthly_rent),
            bigquery.ScalarQueryParameter("tenant", "STRING", prop.tenant_name),
        ]
    )
    client.query(query, job_config=job_config).result()
    return {"message": "Property created successfully", "property_id": next_id}

@app.put("/properties/{property_id}")
def update_property(property_id: int, prop: PropertyCreate):
    query = f"""
        UPDATE `{DATASET}.properties`
        SET name=@p_name, address=@addr, city=@city, state=@state, postal_code=@zip, monthly_rent=@rent, tenant_name=@tenant
        WHERE property_id = @p_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("p_name", "STRING", prop.property_name),
            bigquery.ScalarQueryParameter("addr", "STRING", prop.address),
            bigquery.ScalarQueryParameter("city", "STRING", prop.city),
            bigquery.ScalarQueryParameter("state", "STRING", prop.state),
            bigquery.ScalarQueryParameter("zip", "STRING", prop.zip_code),
            bigquery.ScalarQueryParameter("rent", "FLOAT64", prop.monthly_rent),
            bigquery.ScalarQueryParameter("tenant", "STRING", prop.tenant_name),
        ]
    )
    client.query(query, job_config=job_config).result()
    return {"message": f"Property {property_id} updated successfully"}

@app.delete("/properties/{property_id}")
def delete_property(property_id: int):
    query = f"DELETE FROM `{DATASET}.properties` WHERE property_id = @p_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("p_id", "INT64", property_id)])
    client.query(query, job_config=job_config).result()
    return {"message": f"Property {property_id} deleted successfully"}

@app.delete("/income/record/{income_id}")
def delete_income(income_id: int):
    query = f"DELETE FROM `{DATASET}.income` WHERE income_id = @i_id"
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("i_id", "INT64", income_id)])
    client.query(query, job_config=job_config).result()
    return {"message": f"Income record {income_id} deleted successfully"}