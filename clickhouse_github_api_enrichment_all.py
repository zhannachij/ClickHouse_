from airflow.decorators import task
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import clickhouse_connect

# --- Настройки подключения ---
CLICKHOUSE_HOST = Variable.get("clickhouse_host", default_var="clickhouse-node")
CLICKHOUSE_PORT = Variable.get("clickhouse_port", default_var=8123)
CLICKHOUSE_USER = Variable.get("clickhouse_user", default_var="default")
CLICKHOUSE_PASSWORD = Variable.get("clickhouse_password", default_var="")
GITHUB_TOKEN = Variable.get("github_token", default_var="your_token")  # опционально

# --- Функции ---
@task
def fetch_logins_from_clickhouse(**context):
    """Получить уникальные логины из ClickHouse"""
    from clickhouse_connect import get_client

    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    # Получаем последние 1000 уникальных логинов (чтобы не превысить лимит API)
    query = """
    SELECT DISTINCT ger.actor_login AS actor_login
    FROM github_events_enriched AS ger
    LEFT JOIN github_users_api AS gua
    ON ger.actor_login = gua.login
    WHERE ger.actor_login != ''
    AND gua.login=''
    LIMIT 10000 
    """
    result = client.query(query)
    logins = [row[0] for row in result.result_set if row[0]]
    
    # Сохраняем в XCom для следующей задачи
    #context["task_instance"].xcom_push(key="logins", value=logins)
    print(f"Получено {len(logins)} логинов для обогащения")
    if not logins:
        raise ValueError("No logins found!")
    return logins
@task
def prepare_batches(logins):
    # Получаем logins из XCom предыдущей задачи (по умолчанию — из task_id= fetch_logins_from_clickhouse)
    #logins = context["task_instance"].xcom_pull(task_ids="fetch_logins_from_clickhouse")
    
    #if not logins or not isinstance(logins, list):
    #    raise ValueError("No valid logins received from XCom")

    #batch_size = 50
    #batches = [logins[i:i + batch_size] for i in range(0, len(logins), batch_size)]
    if not isinstance(logins, list) or not logins:
        raise ValueError("Invalid logins input")
    batch_size = 50
    return [logins[i:i+batch_size] for i in range(0, len(logins), batch_size)]
    # Возвращаем список батчей — каждый батч = список логинов
    return batches

@task
def enrich_and_upload_to_clickhouse_batch(batch_logins):
    """Запросить данные из GitHub API и загрузить в ClickHouse"""
    from clickhouse_connect import get_client

    logins = batch_logins#context["task_instance"].xcom_pull(
        #task_ids="fetch_logins_from_clickhouse", key="logins"
    #)
    if not logins:
        print("Нет логинов для обработки")
        return

    # Подготовка заголовков
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    users_data = []
    for i, login in enumerate(logins):
        print(f"Обработка {i+1}/{len(logins)}: {login}")
        try:
            resp = requests.get(
                f"https://api.github.com/users/{login}",
                headers=headers,
                timeout=10
            )
            if resp.status_code == 200:
                user = resp.json()
                users_data.append({
                    "login": user.get("login", ""),
                    "name": user.get("name") or "",
                    "company": (user.get("company") or "").replace("@", "").strip(),
                    "location": user.get("location") or "",
                    "public_repos": user.get("public_repos", 0)
                })
            elif resp.status_code == 403:
                print("⚠️ GitHub API rate limit exceeded")
                break
            # Уважаем rate limit (1 запрос/с без токена, 5/с с токеном)
            import time
            time.sleep(1 if not GITHUB_TOKEN else 0.2)

        except Exception as e:
            print(f"Ошибка при обработке {login}: {e}")
            continue

    if not users_data:
        print("Нет данных для загрузки")
        return

    # Подключаемся к ClickHouse и вставляем
    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    ## Преобразуем в списки колонок
    #logins_col = [u["login"] for u in users_data]
    #names_col = [u["name"] for u in users_data]
    #companies_col = [u["company"] for u in users_data]
    #locations_col = [u["location"] for u in users_data]
    #repos_col = [u["public_repos"] for u in users_data]

    #client.insert(
    #    table="github_users_api",
    #    data=[logins_col, names_col, companies_col, locations_col, repos_col],
    #    column_names=["login", "name", "company", "location", "public_repos"]
    #)
    data = [
        [
            u.get("login"),
            u.get("name"),
            u.get("company"),
            u.get("location"),
            u.get("public_repos")
        ] 
        for u in users_data
    ] 

    client.insert(
        table="github_users_api",
        data=data,
        column_names=["login", "name", "company", "location", "public_repos"]
    ) 
    print(f"✅ Загружено {len(users_data)} записей в github_users_api")

# Параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id="clickhouse_github_api_enrichment_all",
    default_args=default_args,
    description="Enrich GitHub logins from ClickHouse with GitHub API",
    schedule="@daily",
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=["clickhouse", "github", "api"],
) as dag:

    task_fetch_logins = fetch_logins_from_clickhouse()
    batches = prepare_batches(task_fetch_logins)
   
    enrich_and_upload_to_clickhouse_batch.expand(batch_logins=batches)

#from airflow.decorators import task
#from airflow.decorators import task
#from airflow.decorators import task