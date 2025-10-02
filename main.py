from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API is alive!"}

@app.post("/echo")
def echo(data: dict):
    return {"you_sent": data}
