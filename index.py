import pandas as pd
import re
import os
import PyPDF2
from datetime import datetime

def extract_transactions_from_pdf(pdf_path):
    """
    Extrae información de transacciones bancarias desde un PDF de Bancolombia
    y devuelve un DataFrame con los datos estructurados.
    """
    try:
        # Abrir el archivo PDF
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            text = ""
            # Extraer texto de todas las páginas
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
        
        print(f"Procesando archivo: {pdf_path}")
        print("Extrayendo transacciones...")
        
        # Crear listas para almacenar los datos
        transacciones = []
        
        # Limpiar el texto y dividir en líneas
        lines = text.split('\n')
        lines = [line.strip() for line in lines if line.strip()]
        
        # Diccionario para mapear abreviaciones de meses en español
        meses_dict = {
            'ene': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
            'may': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
            'sep': 'Sep', 'oct': 'Oct', 'nov': 'Nov', 'dic': 'Dec'
        }
        
        # Patrones para identificar elementos
        fecha_pattern = r'(\d{1,2}\s+(?:ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)\s+\d{4})'
        valor_pattern = r'([+-]?\$\s*[\d.,]+)'
        
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Buscar fecha al inicio de la línea
            fecha_match = re.search(fecha_pattern, line, re.IGNORECASE)
            
            if fecha_match:
                fecha = fecha_match.group(1)
                
                # Determinar tipo de transacción
                tipo = None
                if 'Crédito' in line:
                    tipo = 'Crédito'
                elif 'Débito' in line:
                    tipo = 'Débito'
                
                if tipo:
                    # Construir la descripción y buscar el valor
                    descripcion_parts = []
                    valor = None
                    
                    # Proceso para extraer descripción y valor
                    # Empezar desde la línea actual
                    j = i
                    lineas_procesadas = 0
                    
                    while j < len(lines) and lineas_procesadas < 5:  # Buscar en máximo 5 líneas
                        current_line = lines[j]
                        
                        # Buscar valor en la línea actual
                        valor_matches = re.findall(valor_pattern, current_line)
                        
                        if valor_matches:
                            # Tomar el último valor encontrado (más probable que sea el correcto)
                            valor = valor_matches[-1]
                            
                            # Extraer descripción (todo menos fecha, tipo y valor)
                            line_clean = current_line
                            line_clean = re.sub(fecha_pattern, '', line_clean, flags=re.IGNORECASE)
                            line_clean = line_clean.replace('Crédito', '').replace('Débito', '')
                            line_clean = line_clean.replace(valor, '')
                            
                            if line_clean.strip():
                                descripcion_parts.append(line_clean.strip())
                            
                            break
                        else:
                            # Si no hay valor, agregar la línea a la descripción
                            line_clean = current_line
                            if j == i:  # Primera línea
                                line_clean = re.sub(fecha_pattern, '', line_clean, flags=re.IGNORECASE)
                                line_clean = line_clean.replace('Crédito', '').replace('Débito', '')
                            
                            # Verificar que no sea el inicio de otra transacción
                            if not re.search(fecha_pattern, line_clean, re.IGNORECASE) and line_clean.strip():
                                descripcion_parts.append(line_clean.strip())
                        
                        j += 1
                        lineas_procesadas += 1
                    
                    # Si encontramos valor, procesar la transacción
                    if valor:
                        descripcion = ' '.join(descripcion_parts)
                        descripcion = re.sub(r'\s+', ' ', descripcion).strip()
                        
                        # Convertir valor a numérico
                        try:
                            valor_limpio = valor.replace('$', '').replace(' ', '').replace('.', '').replace(',', '.')
                            
                            # Manejar signos negativos
                            if valor.startswith('-') or valor_limpio.startswith('-'):
                                valor_limpio = valor_limpio.replace('-', '')
                                valor_numerico = -float(valor_limpio)
                            else:
                                valor_numerico = float(valor_limpio)
                            
                            # Agregar transacción
                            transacciones.append({
                                'Fecha': fecha,
                                'Tipo de transacción': tipo,
                                'Descripción': descripcion,
                                'Valor': valor_numerico
                            })
                            
                            print(f"✓ {fecha} | {tipo} | {descripcion[:50]}... | {valor}")
                            
                        except ValueError as e:
                            print(f"Error procesando valor {valor}: {e}")
            
            i += 1
        
        # Crear DataFrame
        if transacciones:
            df = pd.DataFrame(transacciones)
            
            # Convertir fechas - primero normalizar las abreviaciones
            try:
                def normalizar_fecha(fecha_str):
                    # Convertir abreviaciones españolas a inglesas
                    for esp, eng in meses_dict.items():
                        fecha_str = fecha_str.replace(esp, eng)
                    return fecha_str
                
                df['Fecha_Original'] = df['Fecha']  # Guardar original para debug
                df['Fecha'] = df['Fecha'].apply(normalizar_fecha)
                df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d %b %Y', errors='coerce')
                
                # Mostrar fechas que fallaron en conversión
                fechas_fallidas = df[df['Fecha'].isna()]['Fecha_Original'].unique()
                if len(fechas_fallidas) > 0:
                    print(f"Advertencia: No se pudieron convertir estas fechas: {fechas_fallidas}")
                
                # Limpiar columna temporal
                df = df.drop('Fecha_Original', axis=1)
                
            except Exception as e:
                print(f"Advertencia: Error al convertir fechas: {e}")
            
            # Ordenar por fecha (más reciente primero)
            df = df.sort_values(by='Fecha', ascending=False).reset_index(drop=True)
            
            return df
        else:
            print("No se encontraron transacciones en el PDF")
            return pd.DataFrame(columns=['Fecha', 'Tipo de transacción', 'Descripción', 'Valor'])
            
    except Exception as e:
        print(f"Error procesando el PDF: {e}")
        return pd.DataFrame(columns=['Fecha', 'Tipo de transacción', 'Descripción', 'Valor'])

def save_to_excel(df, output_path):
    """
    Guarda el DataFrame en un archivo Excel con formato adecuado.
    """
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # Escribir el DataFrame en el archivo Excel
            df.to_excel(writer, index=False, sheet_name='Movimientos')
            
            # Obtener el libro y la hoja
            workbook = writer.book
            worksheet = writer.sheets['Movimientos']
            
            # Definir formatos
            formato_fecha = workbook.add_format({'num_format': 'dd/mm/yyyy'})
            formato_moneda = workbook.add_format({'num_format': '$#,##0.00'})
            formato_header = workbook.add_format({
                'bold': True,
                'bg_color': '#366092',
                'font_color': 'white',
                'border': 1
            })
            
            # Aplicar formato a los headers
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, formato_header)
            
            # Aplicar formatos a las columnas
            worksheet.set_column('A:A', 12, formato_fecha)  # Fecha
            worksheet.set_column('B:B', 18)  # Tipo de transacción
            worksheet.set_column('C:C', 80)  # Descripción
            worksheet.set_column('D:D', 15, formato_moneda)  # Valor
            
            print(f"✓ Archivo Excel guardado: {output_path}")
            
    except Exception as e:
        print(f"Error guardando el archivo Excel: {e}")

def process_pdf_to_excel(pdf_path, output_path=None):
    """
    Función principal para procesar un PDF y convertirlo a Excel
    """
    if not output_path:
        # Generar nombre de archivo Excel basado en el PDF
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        output_path = f"{base_name}_Movimientos.xlsx"
    
    # Verificar que el archivo PDF existe
    if not os.path.exists(pdf_path):
        print(f"❌ Error: El archivo '{pdf_path}' no existe.")
        return None
    
    # Extraer datos del PDF
    df = extract_transactions_from_pdf(pdf_path)
    
    if df.empty:
        print("❌ No se pudieron extraer transacciones del PDF.")
        return None
    
    # Guardar en Excel
    save_to_excel(df, output_path)
    
    # Mostrar resumen
    print(f"\n📊 Resumen:")
    print(f"   • Transacciones procesadas: {len(df)}")
    print(f"   • Archivo de salida: {output_path}")
    
    # Verificar si hay fechas válidas
    fechas_validas = df['Fecha'].notna()
    if fechas_validas.any():
        print(f"   • Rango de fechas: {df[fechas_validas]['Fecha'].min().strftime('%d/%m/%Y')} - {df[fechas_validas]['Fecha'].max().strftime('%d/%m/%Y')}")
    else:
        print("   • Advertencia: No se pudieron procesar las fechas correctamente")
    
    # Mostrar primeras transacciones
    print(f"\n🔍 Primeras 5 transacciones:")
    for idx, row in df.head().iterrows():
        if pd.notnull(row['Fecha']):
            fecha_str = row['Fecha'].strftime('%d/%m/%Y')
        else:
            fecha_str = 'Fecha inválida'
        print(f"   {fecha_str} | {row['Tipo de transacción']} | ${row['Valor']:,.2f}")
    
    return df

# Ejemplo de uso
if __name__ == "__main__":
    # Nombre del archivo PDF a procesar
    pdf_file = "archivo.pdf"  # Cambia este nombre por tu archivo
    
    # Procesar el PDF
    df = process_pdf_to_excel(pdf_file)
    
    # También puedes especificar el nombre del archivo de salida
    # df = process_pdf_to_excel("mi_archivo.pdf", "mi_salida.xlsx")