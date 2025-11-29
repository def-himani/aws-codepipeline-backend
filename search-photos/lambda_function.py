import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

region = "us-east-1"
service = "es"

# --- Replace with your OpenSearch domain endpoint ---
es_host = "search-photos-xyfnvddh2ulus663bd4vesnvju.us-east-1.es.amazonaws.com"

# AWS Clients
lex_client = boto3.client("lexv2-runtime")
session = boto3.Session()
creds = session.get_credentials().get_frozen_credentials()

awsauth = AWS4Auth(
    creds.access_key,
    creds.secret_key,
    region,
    service,
    session_token=creds.token
)

# OpenSearch Client
es = OpenSearch(
    hosts=[{"host": es_host, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)

# --- REPLACE WITH YOUR LEX BOT INFO ---
BOT_ID = "KPONLNRFCS"
BOT_ALIAS_ID = "IDEVCDHCUL"
LOCALE_ID = "en_US"


def lambda_handler(event, context):
    """
    Handles search requests:
    - Extracts query text
    - Sends to Lex to extract keywords
    - Searches OpenSearch for matching labels
    """

    print("EVENT RECEIVED:", json.dumps(event))

    # Support API Gateway V1 and V2 payload formats
    if "queryStringParameters" in event:
        query = event["queryStringParameters"].get("q", "")
    else:
        # direct invocation fallback
        query = event.get("q", "")

    if not query:
        return _response({"results": []})

    # -------------------------
    # 1️⃣ Send query to Lex V2
    # -------------------------
    try:
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId="lambda-session",
            text=query
        )
    except Exception as e:
        print("LEX ERROR:", str(e))
        return _response({"results": []})

    print("LEX RESPONSE:", json.dumps(lex_response))

    # Extract slots
    interpretations = lex_response.get("interpretations", [])
    if not interpretations:
        return _response({"results": []})

    intent_slots = interpretations[0].get("intent", {}).get("slots", {})

    # Slot named "Keywords" must match your Lex intent slot name
    slot = intent_slots.get("Keywords")

    if not slot or "value" not in slot:
        return _response({"results": []})

    keywords_text = slot["value"]["interpretedValue"]

    if not keywords_text:
        return _response({"results": []})

    # Handle multi-word keywords ("dogs and cats")
    keyword_list = [w.strip().lower() for w in keywords_text.replace(",", " ").split() if w.strip()]

    print("EXTRACTED KEYWORDS:", keyword_list)

    # -------------------------------------
    # 2️⃣ Build OpenSearch Query for labels
    # -------------------------------------
    es_query = {
        "query": {
            "bool": {
                "should": [{"match": {"labels": kw}} for kw in keyword_list],
                "minimum_should_match": 1
            }
        }
    }

    # -------------------------
    # 3️⃣ Execute OpenSearch Query
    # -------------------------
    try:
        search_results = es.search(index="photos", body=es_query)
    except Exception as e:
        print("OPENSEARCH ERROR:", str(e))
        return _response({"results": []})

    hits = search_results.get("hits", {}).get("hits", [])

    # -------------------------
    # 4️⃣ Format Response
    # -------------------------
    results = []
    for hit in hits:
        src = hit["_source"]
        results.append({
            "objectKey": src.get("objectKey"),
            "bucket": src.get("bucket"),
            "labels": src.get("labels", [])
        })

    return _response({"results": results})


# ---------------------------------
# Helper: CORS + JSON response
# ---------------------------------
def _response(body_dict, status_code=200):
    return {
        "statusCode": status_code,
        "headers": {
            # allow any origin (for assignment/demo). For production, restrict to your frontend URL.
            "Access-Control-Allow-Origin": "*",
            # include headers your browser/API will send
            "Access-Control-Allow-Headers": "Content-Type, X-Amz-Date, Authorization, X-Api-Key, x-amz-meta-customLabels, x-api-key",
            "Access-Control-Allow-Methods": "OPTIONS,GET,PUT,POST,DELETE",
            # helpful for some clients
            "Access-Control-Max-Age": "3600"
        },
        "body": json.dumps(body_dict)
    }
