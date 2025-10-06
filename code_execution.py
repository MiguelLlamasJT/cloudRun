import os
import tempfile
import anthropic
import pandas as pd

claude = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

def run_code_execution(prompt: str, df: pd.DataFrame, model: str = "claude-sonnet-4-20250514") -> str:
    if df.empty:
        return("No data available.")
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp_file:
        df.to_csv(tmp_file.name, index=False)
        tmp_path = tmp_file.name
    
    print("file created at " + tmp_path)

    with open(tmp_path, "rb") as f:
        uploaded = claude.beta.files.upload(file=("data.csv", f, "text/csv"))
    try:
        response = claude.beta.messages.create(
            model=model,
            betas=["code-execution-2025-08-25", "files-api-2025-04-14", "context-1m-2025-08-07"],
            max_tokens=4096,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Using the info in the file provided: " + prompt},
                    {"type": "container_upload", "file_id": uploaded.id}
                ]
            }],
            tools=[{"type": "code_execution_20250825", "name": "code_execution"}]
        )
        print(response)
        output_text = "".join(
            item.text for item in response.content if getattr(item, "type", None) == "text"
        )
        return output_text.strip()
    finally:
        try:
            claude.beta.files.delete(uploaded.id)
            print("deleted file")
        except Exception as e:
            print(f"Could not delete file: {e}")
        try:
            os.remove(tmp_path)
            print("removed path")
        except Exception as e:
            print(f"could not delete path: {e}")