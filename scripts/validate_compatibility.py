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

def analizar_campos_recursiva(old_fields, new_fields, path=""):
    cambios = {
        'añadidos_sin_default': [],
        'eliminados_sin_default': [],
        'modificados': []
    }

    old_map = {f.name: f for f in old_fields}
    new_map = {f.name: f for f in new_fields}

    for name in new_map:
        full_path = f"{path}{name}"
        if name not in old_map:
            if not new_map[name].has_default:
                cambios['añadidos_sin_default'].append(full_path)
        else:
            old_type = old_map[name].type
            new_type = new_map[name].type

            # Cambio de tipo
            if str(old_type) != str(new_type):
                cambios['modificados'].append(f"{full_path} (type: {old_type} -> {new_type})")
                continue

            # Cambio de valor por defecto
            if old_map[name].has_default != new_map[name].has_default:
                cambios['modificados'].append(f"{full_path} (default changed)")

            # Si el tipo es record, analizamos subcampos
            if hasattr(old_type, "fields") and hasattr(new_type, "fields"):
                sub_cambios = analizar_campos_recursiva(old_type.fields, new_type.fields, f"{full_path}.")
                for key in cambios:
                    cambios[key].extend(sub_cambios[key])

    # Campos eliminados
    for name in old_map:
        if name not in new_map and not old_map[name].has_default:
            cambios['eliminados_sin_default'].append(f"{path}{name}")

    return cambios


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
        subject = "store-orders-value"

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
        # Validar campos recursivamente
        cambios_campos = analizar_campos_recursiva(esquema_ant.fields, esquema_nuevo.fields)
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

'''
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
        subject = "store-orders-value"

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
'''