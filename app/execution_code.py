import os, tempfile
import time
import pandas as pd
from threading import Event
from concurrent.futures import ThreadPoolExecutor
from app import claude, logger
from app.utils_slack.slack_utils import update_message, uploadFiles, completeUpload
from app.utils_slack.format_utils import format_for_slack

def run_code_execution(prompt: str, df: pd.DataFrame, channel: str, user: str, threadts: str, model: str = "claude-sonnet-4-5-20250929") -> str:  #claude-3-5-haiku-latest claude-sonnet-4-20250514
    if df.empty:
        return("No data available.")
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp_file:
        df.to_csv(tmp_file.name, index=False)
        tmp_path = tmp_file.name
    
    logger.debug("file created at " + tmp_path)

    with open(tmp_path, "rb") as f:
            uploaded = claude.beta.files.upload(file=("data.csv", f, "text/csv"))
    try:
        update_message(channel=channel, ts=threadts, new_text="🔬Analyzing...")
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
        file_ids = []
        
        for block in response.content:
            if getattr(block, "type", "") == "bash_code_execution_tool_result":
                result_block = getattr(block, "content", None)
                if result_block:
                    for output in getattr(result_block, "content", []):
                        file_id = getattr(output, "file_id", None)
                        if file_id:
                            file_ids.append(file_id)
        print(file_ids)
        final_ids = []
        for id in file_ids:
            logger.debug(id)
            file_response = claude.beta.files.download(id)
            data = claude.beta.files.retrieve_metadata(id)
            file_id = uploadFiles(file_response, data.filename)
            final_ids.append({"id": file_id, "title": data.filename})

        logger.debug(response)

        text_blocks = [item.text for item in response.content if getattr(item, "type", None) == "text"]
        output_text = text_blocks[-1] if text_blocks else ""
        output_text = output_text.strip()

        logger.debug("code execution did not fail")
        input_tokens = "\n\nInput tokens: " + str(response.usage.input_tokens) + " - Cost €: " + str(int(response.usage.input_tokens) * 0.86 /  3000000)
        logger.debug(input_tokens)
        output = format_for_slack(output_text + input_tokens)
        if len(final_ids) > 0 :
            completeUpload(channel, threadts, final_ids, format_for_slack(output_text+input_tokens))
            return "Analysis Completed"
        return output
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