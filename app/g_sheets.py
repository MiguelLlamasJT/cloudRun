from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets"
]


def create_sheet(values, filename="P&L generado automáticamente", share_with_email=None):
    """
    Crea una Google Spreadsheet usando la service account (Cloud Run / ADC),
    escribe los valores en A1 y la comparte con un email opcional.

    Returns:
        dict: { "spreadsheet_id": str, "url": str }
    """

    values = [
        ["col1", "col2"],
        ["a", 1],
        ["b", 2],
    ]


    try:
        # 1. Autenticación por ADC (service account del Cloud Run)
        creds, project_id = google.auth.default(scopes=SCOPES)
    except Exception as e:
        raise RuntimeError(f"Error obteniendo credenciales ADC: {str(e)}")

    try:
        drive = build("drive", "v3", credentials=creds)
        sheets = build("sheets", "v4", credentials=creds)
    except Exception as e:
        raise RuntimeError(f"Error inicializando clientes Drive/Sheets: {str(e)}")

    # ---------------------------------------------------------
    # 2. Crear el fichero tipo Google Sheets (en My Drive SA)
    # ---------------------------------------------------------
    file_metadata = {
        "name": filename,
        "mimeType": "application/vnd.google-apps.spreadsheet"
        # No parents -> cae en My Drive de la service account
    }

    try:
        file = drive.files().create(
            body=file_metadata,
            fields="id"
        ).execute()

        spreadsheet_id = file["id"]
    except HttpError as e:
        raise RuntimeError(f"Error creando la spreadsheet: {e.error_details}")
    except Exception as e:
        raise RuntimeError(f"Error inesperado creando la spreadsheet: {str(e)}")

    # ---------------------------------------------------------
    # 3. Compartir el fichero con el email (si se pidió)
    # ---------------------------------------------------------
    if share_with_email:
        try:
            drive.permissions().create(
                fileId=spreadsheet_id,
                body={
                    "type": "user",
                    "role": "writer",
                    "emailAddress": share_with_email
                },
                fields="id"
            ).execute()
        except HttpError as e:
            raise RuntimeError(
                f"Spreadsheet creada pero error compartiéndola con {share_with_email}: {e.error_details}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Spreadsheet creada pero error inesperado compartiéndola con {share_with_email}: {str(e)}"
            )

    # ---------------------------------------------------------
    # 4. Escribir los valores en la primera hoja ("Hoja 1")
    # ---------------------------------------------------------
    try:
        sheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Hoja 1!A1",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
    except HttpError as e:
        raise RuntimeError(f"Error escribiendo datos en la hoja: {e.error_details}")
    except Exception as e:
        raise RuntimeError(f"Error inesperado escribiendo datos: {str(e)}")

    # URL para abrir en navegador
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

    return {
        "spreadsheet_id": spreadsheet_id,
        "url": url
    }
