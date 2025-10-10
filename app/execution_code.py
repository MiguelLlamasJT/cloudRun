import os, tempfile
import pandas as pd
from app import claude, logger

def run_code_execution(prompt: str, df: pd.DataFrame, model: str = "claude-sonnet-4-20250514") -> str:
    if df.empty:
        return("No data available.")
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp_file:
        df.to_csv(tmp_file.name, index=False)
        tmp_path = tmp_file.name
    
    logger.debug("file created at " + tmp_path)

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
                    {"type": "text", "text": "Based on the attached file and the provided prompt, generate an answer that is concise (approximately 60-per-cent condensed) but still retains essential details, in response to the following question:" + prompt},
                    {"type": "container_upload", "file_id": uploaded.id}
                ]
            }],
            tools=[{"type": "code_execution_20250825", "name": "code_execution"}]
        )
        logger.debug(response)
        text_blocks = [item.text for item in response.content if getattr(item, "type", None) == "text"]
        output_text = text_blocks[-1] if text_blocks else ""
        output_text = output_text.strip()
        logger.debug("code execution did not fail")
        input_tokens = "\n\nInput tokens: " + str(response.usage.input_tokens)
        logger.debug(input_tokens)
        return output_text + input_tokens
    finally:
        try:
            claude.beta.files.delete(uploaded.id)
            logger.debug("deleted file")
        except Exception as e:
            logger.error(f"Could not delete file: {e}")
        try:
            os.remove(tmp_path)
            logger.debug("removed path")
        except Exception as e:
            logger.error(f"could not delete path: {e}")