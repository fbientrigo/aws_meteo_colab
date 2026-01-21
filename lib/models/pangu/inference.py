import os
import numpy as np
import onnx
import onnxruntime as ort
from datetime import timedelta

class PanguInference:
    """
    Clase modular para manejar la inferencia con Pangu-Weather.
    Soporta carga única del modelo y ejecución iterativa.
    """
    def __init__(self, model_path: str):
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"No se encontró el modelo en: {model_path}")

        self.model_path = model_path
        self.session = self._init_session()

    def _init_session(self) -> ort.InferenceSession:
        print(f"[PanguInference] Cargando modelo: {self.model_path}")

        # Opciones de sesión para optimizar memoria
        options = ort.SessionOptions()
        options.enable_cpu_mem_arena = False
        options.enable_mem_pattern = False
        options.enable_mem_reuse = False
        options.intra_op_num_threads = 4  # Ajustar según recursos

        # Proveedores: Intentar CUDA primero, luego CPU
        cuda_options = {'arena_extend_strategy': 'kSameAsRequested'}
        providers = [('CUDAExecutionProvider', cuda_options), 'CPUExecutionProvider']

        session = ort.InferenceSession(self.model_path, sess_options=options, providers=providers)
        print(f"[PanguInference] Modelo cargado en: {session.get_providers()[0]}")
        return session

    def predict_step(self, input_upper: np.ndarray, input_surface: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """
        Ejecuta un paso de predicción (24h).
        Inputs deben ser float32.
        """
        output_upper, output_surface = self.session.run(
            None,
            {
                'input': input_upper.astype(np.float32),
                'input_surface': input_surface.astype(np.float32)
            }
        )
        return output_upper, output_surface

    def run_sequence(
        self,
        input_upper_init: np.ndarray,
        input_surface_init: np.ndarray,
        start_time,
        steps: int = 1,
        callback = None
    ) -> list:
        """
        Ejecuta una secuencia de predicciones iterativas.

        Args:
            steps (int): Número de pasos (días) a predecir.
            callback (func): Función (step, upper, sfc, valid_time) -> None a ejecutar tras cada paso.
        """
        curr_upper = input_upper_init
        curr_surface = input_surface_init
        curr_time = start_time

        history = []

        for i in range(1, steps + 1):
            print(f"[PanguInference] Ejecutando paso {i}/{steps}...")

            # Inferencia
            curr_upper, curr_surface = self.predict_step(curr_upper, curr_surface)
            curr_time += timedelta(hours=24)

            # Ejecutar callback (procesamiento modular por paso)
            if callback:
                callback(
                    step=i,
                    pred_upper=curr_upper,
                    pred_surface=curr_surface,
                    valid_time=curr_time
                )

            history.append((curr_time, curr_upper, curr_surface))

        return history