#!/usr/bin/env python3
import sys
import requests
from avro.schema import parse

def validar_metadatos(cambios_metadatos, compatibilidad):
    errores = []
    advertencias = []

    # Reglas para 'name'
    if 'name' in cambios_metadatos:
        if compatibilidad != 'NONE':
            errores.append("Cambio de 'name' requiere compatibilidad=NONE")
        else:
            advertencias.append("Cambio de 'name' detectado (usar aliases para compatibilidad)")

    # Reglas para 'type'
    if 'type' in cambios_metadatos:
        errores.append("Cambio de 'type' es incompatible con cualquier modo de compatibilidad")

    # Reglas para 'namespace'
    if 'namespace' in cambios_metadatos:
        advertencias.append("Cambio de 'namespace' puede afectar serialización (usar aliases)")

    return errores, advertencias

def obtener_compatibilidad(url_registry, subject):
    try:
        response = requests.get(f"{url_registry}/config/{subject}")
        if response.status_code == 200:
            return response.json()['compatibilityLevel']

        response_global = requests.get(f"{url_registry}/config")
        return response_global.json().get('compatibilityLevel', 'BACKWARD')

    except Exception as e:
        print(f"⚠️ Error obteniendo compatibilidad: {e}")
        return 'BACKWARD'

def analizar_campos(esquema_ant, esquema_nuevo):
    campos_ant = {c.name: c for c in esquema_ant.fields}
    campos_nue = {c.name: c for c in esquema_nuevo.fields}

    return {
        'añadidos_sin_default': [n for n in campos_nue if n not in campos_ant and not campos_nue[n].has_default],
        'eliminados_sin_default': [n for n in campos_ant if n not in campos_nue and not campos_ant[n].has_default]
    }

def validar_reglas_campos(cambios_campos, compatibilidad):
    requerida = None

    if cambios_campos['eliminados_sin_default']:
        requerida = 'BACKWARD' if 'BACKWARD' in compatibilidad else None
    elif cambios_campos['añadidos_sin_default']:
        requerida = 'FORWARD' if 'FORWARD' in compatibilidad else None

    return requerida

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python validate_compatibility.py <esquema_ant> <esquema_nuevo>")
        sys.exit(1)

    try:
        # Cargar esquemas
        esquema_ant = parse(open(sys.argv[1]).read())
        esquema_nuevo = parse(open(sys.argv[2]).read())

        # Configuración
        registry_url = "http://schema-registry:8081"
        subject = "store-orders"

        # Obtener compatibilidad
        compatibilidad = obtener_compatibilidad(registry_url, subject)
        print(f"🔍 Modo de compatibilidad: {compatibilidad}")

        # Validar metadatos
        cambios_metadatos = {
            'type': (esquema_ant.type, esquema_nuevo.type),
            'name': (esquema_ant.name, esquema_nuevo.name),
            'namespace': (esquema_ant.namespace, esquema_nuevo.namespace),
            'doc': (getattr(esquema_ant, 'doc', None), getattr(esquema_nuevo, 'doc', None))
        }
        cambios_metadatos = {k: v for k, v in cambios_metadatos.items() if v[0] != v[1]}

        errores, advertencias = validar_metadatos(cambios_metadatos, compatibilidad)

        # Validar campos
        cambios_campos = analizar_campos(esquema_ant, esquema_nuevo)
        compatibilidad_requerida = validar_reglas_campos(cambios_campos, compatibilidad)

        # Resultados
        if errores:
            print("❌ Errores de compatibilidad:")
            for e in errores: print(f"  - {e}")

        if advertencias:
            print("⚠️ Advertencias:")
            for a in advertencias: print(f"  - {a}")

        if compatibilidad_requerida and compatibilidad_requerida not in compatibilidad:
            print(f"❌ Compatibilidad requerida: {compatibilidad_requerida}")
            sys.exit(1)

        if errores:
            sys.exit(1)

        print("✅ Validación completada exitosamente")
        sys.exit(0)

    except Exception as e:
        print(f"❌ Error crítico: {e}")
        sys.exit(1)