from locust import HttpUser, task, between

class CryptoUser(HttpUser):
    # Simula el tiempo que un usuario real se queda mirando la gráfica (entre 1 y 5 segundos)
    wait_time = between(1, 5)

    @task
    def ver_dashboard(self):
        # Ataca la página principal de tu Grafana
        self.client.get("/")
        
    @task(3) # Este número hace que esta tarea se ejecute 3 veces más seguido
    def consultar_api_grafana(self):
        # Simula las peticiones que hace Grafana por detrás para actualizar las gráficas
        self.client.get("/api/health")