# drift_config.py
from datetime import datetime, timedelta

# Punto de inflexión: 6 meses después del inicio
DRIFT_START_DAY = 180  # Día 180 de 365
DRIFT_FULL_DAY = 270   # Drift completo al día 270

class DriftConfig:
    """Configuración de drift progresivo en el comportamiento de churn"""
    
    @staticmethod
    def get_drift_factor(day_index):
        """
        Retorna un factor entre 0 (sin drift) y 1 (drift completo)
        """
        if day_index < DRIFT_START_DAY:
            return 0.0
        elif day_index >= DRIFT_FULL_DAY:
            return 1.0
        else:
            # Transición suave
            progress = (day_index - DRIFT_START_DAY) / (DRIFT_FULL_DAY - DRIFT_START_DAY)
            return progress
    
    @staticmethod
    def get_churn_probs(day_index):
        """
        Probabilidades de churn que cambian con el tiempo
        """
        drift = DriftConfig.get_drift_factor(day_index)
        
        # Estado inicial (meses 1-6)
        initial = {
            "free": 0.30,
            "basic": 0.10,
            "premium": 0.05
        }
        
        # Estado final (después mes 9)
        # Cambio: Basic sufre más, Premium se mantiene
        final = {
            "free": 0.25,      # Mejora ligeramente
            "basic": 0.20,     # Se duplica (cambio de mercado)
            "premium": 0.04    # Mejora ligeramente
        }
        
        # Interpolación
        return {
            plan: initial[plan] * (1 - drift) + final[plan] * drift
            for plan in initial.keys()
        }
    
    @staticmethod
    def get_inactivity_threshold(day_index):
        """
        Umbral de inactividad que cambia con el tiempo
        """
        drift = DriftConfig.get_drift_factor(day_index)
        
        # Inicialmente 14 días, luego 7 días (usuarios más impacientes)
        return int(14 * (1 - drift) + 7 * drift)
    
    @staticmethod
    def get_payment_importance(day_index):
        """
        Importancia de pagos fallidos cambia con el tiempo
        """
        drift = DriftConfig.get_drift_factor(day_index)
        
        # Inicialmente 3x, luego 5x (más sensibilidad a problemas de pago)
        return 3.0 * (1 - drift) + 5.0 * drift
    
    @staticmethod
    def get_ticket_importance(day_index):
        """
        Importancia de tickets sin resolver
        """
        drift = DriftConfig.get_drift_factor(day_index)
        
        # Inicialmente 1.5x, luego 2.5x (más sensibilidad al soporte)
        return 1.5 * (1 - drift) + 2.5 * drift