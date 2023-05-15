from typing import Any, Dict

import requests
import json


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AirflowAPIService(metaclass=Singleton):

    def __init__(self) -> None:
        self.username = 'airflow'
        self.password = 'airflow'
        self.base_url = f"http://localhost:8080/api/v1/"
        self.auth = (self.username, self.password)
        self.headers = {'accept': 'application/json', 'Content-Type': 'application/json'}

    def get_dags(self) -> Dict[str, Any]:
        response = requests.get(f"{self.base_url}dags", auth=self.auth, headers=self.headers)
        return json.loads(response.content)

    def trigger_dag_run(self, dag_id: str, dag_run_id: str, conf: Dict[str, Any] = None) -> Dict[str, Any]:
        response = requests.post(f"{self.base_url}dags/{dag_id}/dagRuns", auth=self.auth, headers=self.headers,
                                 data=json.dumps({
                                     "dag_run_id": dag_run_id,
                                     "conf": conf if conf else {}
                                 }))
        return json.loads(response.content)

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        response = requests.get(f"{self.base_url}dags/{dag_id}/dagRuns/{dag_run_id}", auth=self.auth,
                                headers=self.headers)
        return json.loads(response.content)

    def unpause_dags(self, dag_id: str) -> Dict[str, Any]:
        response = requests.patch(f"{self.base_url}dags/{dag_id}", auth=self.auth, headers=self.headers,
                                  data=json.dumps({"is_paused": False}))
        return json.loads(response.content)
