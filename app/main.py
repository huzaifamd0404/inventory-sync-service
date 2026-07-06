from fastapi import FastAPI


app = FastAPI(title="Inventory Sync Service")


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "Inventory Sync Service is running"}
