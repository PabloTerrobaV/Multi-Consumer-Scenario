#!/usr/bin/env python3
import json
import sys
import os
from avro.schema import parse, Schema

def cargar_esquema(archivo):
    if not os.path.exists(archivo):
        raise FileNotFoundError(f"El archivo '{archivo}' no existe.")

    with open(archivo, 'r') as f:
        contenido = f.read().strip()

    if not contenido:
        raise ValueError(f"El archivo '{archivo}' está vacío.")

    try:
        return parse(contenido)
    except Exception as e:
        raise ValueError(f"Error al parsear '{archivo}': {e}")

def comparar_metadatos(esquema1: Schema, esquema2: Schema):
    metadatos = ['type', 'name', 'namespace', 'doc']
    diferencias = {}

    for attr in metadatos:
        val1 = getattr(esquema1, attr, None)
        val2 = getattr(esquema2, attr, None)

        if val1 != val2:
            diferencias[attr] = {'anterior': val1, 'nuevo': val2}

    return diferencias

def comparar_campos(esquema_ant: Schema, esquema_nuevo: Schema):
    campos_ant = {c.name: c for c in esquema_ant.fields}
    campos_nue = {c.name: c for c in esquema_nuevo.fields}

    cambios = {
        'añadidos': [],
        'eliminados': [],
        'modificados': []
    }

    # Campos añadidos
    for nombre in campos_nue:
        if nombre not in campos_ant:
            cambios['añadidos'].append(analizar_campo(campos_nue[nombre]))

    # Campos eliminados
    for nombre in campos_ant:
        if nombre not in campos_nue:
            cambios['eliminados'].append(analizar_campo(campos_ant[nombre]))

    # Campos modificados
    for nombre in campos_ant:
        if nombre in campos_nue:
            ant = campos_ant[nombre]
            nue = campos_nue[nombre]
            if ant != nue:
                cambios['modificados'].append({
                    'nombre': nombre,
                    'anterior': analizar_campo(ant),
                    'nuevo': analizar_campo(nue)
                })

    return cambios

def analizar_campo(campo):
    return {
        'nombre': campo.name,
        'tipo': str(campo.type),
        'doc': getattr(campo, 'doc', None),
        'default': campo.default if hasattr(campo, 'default') else None,
        'orden': getattr(campo, 'order', None)
    }

def generar_reporte(cambios):
    reporte = []

    # Metadatos
    if cambios['metadatos']:
        reporte.append("=== CAMBIOS EN METADATOS ===")
        for attr, vals in cambios['metadatos'].items():
            reporte.append(f"🔵 {attr.upper()}:")
            reporte.append(f"  Anterior: {vals['anterior']}")
            reporte.append(f"  Nuevo:    {vals['nuevo']}")

    # Campos
    if cambios['campos']['añadidos']:
        reporte.append("\n🟢 CAMPOS AÑADIDOS:")
        for campo in cambios['campos']['añadidos']:
            reporte.append(formatear_campo(campo))

    if cambios['campos']['eliminados']:
        reporte.append("\n🔴 CAMPOS ELIMINADOS:")
        for campo in cambios['campos']['eliminados']:
            reporte.append(formatear_campo(campo))

    if cambios['campos']['modificados']:
        reporte.append("\n🟠 CAMPOS MODIFICADOS:")
        for cambio in cambios['campos']['modificados']:
            reporte.append(f"  ~ {cambio['nombre']}:")
            reporte.append("    Anterior: " + formatear_campo(cambio['anterior']))
            reporte.append("    Nuevo:    " + formatear_campo(cambio['nuevo']))

    # Resumen
    # total = sum(len(v) for v in cambios['metadatos'].values()) + \
    #  sum(len(v) for v in cambios['campos'].values())

    # Resumen final
    total_metadatos = len(cambios['metadatos'])
    total_campos = (
            len(cambios['campos']['añadidos']) +
            len(cambios['campos']['eliminados']) +
            len(cambios['campos']['modificados'])
    )

    reporte.append("\n=== RESUMEN ===")
    reporte.append(f"Metadatos modificados: {total_metadatos}")
    reporte.append(f"Campos añadidos: {len(cambios['campos']['añadidos'])}")
    reporte.append(f"Campos eliminados: {len(cambios['campos']['eliminados'])}")
    reporte.append(f"Campos modificados: {len(cambios['campos']['modificados'])}")
    reporte.append(f"Total de cambios: {total_metadatos + total_campos}")

    # reporte.append("\n=== RESUMEN ===")
    # reporte.append(f"Metadatos modificados: {len(cambios['metadatos'])}")
    # reporte.append(f"Campos añadidos: {len(cambios['campos']['añadidos'])}")
    # reporte.append(f"Campos eliminados: {len(cambios['campos']['eliminados'])}")
    # reporte.append(f"Campos modificados: {len(cambios['campos']['modificados'])}")
    # reporte.append(f"Total de cambios: {total}")

    return '\n'.join(reporte)

def formatear_campo(campo):
    detalles = []
    if campo['doc']: detalles.append(f"Doc: {campo['doc']}")
    if campo['default'] is not None: detalles.append(f"Default: {campo['default']}")
    if campo['orden']: detalles.append(f"Orden: {campo['orden']}")
    return f"{campo['nombre']} ({campo['tipo']})" + (f" [{', '.join(detalles)}]" if detalles else "")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python compare_schemas.py <esquema_anterior> <esquema_nuevo>")
        sys.exit(1)

    try:
        esquema_ant = cargar_esquema(sys.argv[1])
        esquema_nuevo = cargar_esquema(sys.argv[2])

        cambios = {
            'metadatos': comparar_metadatos(esquema_ant, esquema_nuevo),
            'campos': comparar_campos(esquema_ant, esquema_nuevo)
        }

        print(generar_reporte(cambios))
        sys.exit(0)

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)