import streamlit as st
import pydeck as pdk
import pandas as pd
import numpy as np
from src.api_client import MeteoAPI

# Page Config
st.set_page_config(page_title="AWS Meteo Colab", layout="wide")

# --- FUNCIONES AUXILIARES CON CACHÉ ---
# Usamos caché para no llamar a la API cada vez que se refresca la UI
@st.cache_data(ttl=600)  # Cache por 10 minutos
def get_runs_cached(_api):
    return _api.get_available_runs()

@st.cache_data(ttl=600)
def get_steps_cached(_api, run):
    return _api.get_steps_for_run(run)

@st.cache_data(show_spinner=False)
def get_spatial_data_cached(_api, run, step):
    return _api.get_spatial_data(run, step)

def render_map(df, data_col="sti"):
    """
    Renderiza el mapa con colores dinámicos basados en la columna de datos.
    data_col: El nombre de la columna que contiene el valor a pintar (ej. 'sti', 't2m', etc)
    """
    if df is not None and not df.empty:
        # Limpieza preventiva
        df = df.dropna(subset=["lat", "lon", data_col])

        if df.empty:
            st.warning("El dataset está vacío después de limpiar coordenadas nulas.")
            return

        # 1. Lógica de Colores (Normalización)
        min_val = df[data_col].min()
        max_val = df[data_col].max()
        denom = (max_val - min_val) if max_val > min_val else 1.0

        def get_color(val):
            norm = (val - min_val) / denom
            r = int(255 * norm)
            g = int(255 * (1 - norm))
            return [r, g, 0, 180] 

        df["color"] = df[data_col].apply(get_color)

        # 2. CÁLCULO INTELIGENTE DE RESOLUCIÓN (Para rellenar huecos)
        # Estimamos la distancia promedio entre puntos de la grilla
        unique_lats = np.sort(df["lat"].unique())
        if len(unique_lats) > 1:
            # Mediana de las diferencias entre latitudes únicas para encontrar el paso de la grilla
            grid_res_deg = np.median(np.diff(unique_lats))
            # Convertir grados aprox a metros (1 deg lat ~= 111,139 metros)
            # Usamos un factor de 1.1 para asegurar un ligero solapamiento y evitar líneas blancas
            radius_full_fill = (grid_res_deg * 111139) / 2 * 1.5
        else:
            radius_full_fill = 10000 # Fallback 10km

        # OPCIÓN DE CONTROL VISUAL
        col_ctrl1, col_ctrl2 = st.columns([1, 3])
        with col_ctrl1:
            # Toggle para activar el "Relleno" (Grid Fill)
            fill_mode = st.toggle("🟦 Rellenar Huecos (Full Grid)", value=True, 
                                help="Calcula el tamaño real de la celda de la grilla y expande los puntos para crear una superficie continua.")

        # Configuración dinámica del radio
        if fill_mode:
            # MODO RELLENO: El radio depende de la resolución de los datos
            radius_settings = {
                "get_radius": radius_full_fill, 
                "radius_units": "'meters'",
                "radius_min_pixels": 2,
                "radius_max_pixels": None, # Sin límite para que crezca al hacer zoom
            }
            st.caption(f"ℹ️ Resolución detectada: {radius_full_fill/1000:.2f} km por punto.")
        else:
            # MODO PUNTOS DISCRETOS
            radius_settings = {
                "get_radius": 5000,        
                "radius_units": "'meters'",
                "radius_min_pixels": 3,
                "radius_max_pixels": 15,
            }

        # 3. Configuración PyDeck
        layer = pdk.Layer(
            "ScatterplotLayer",
            df,
            get_position=["lon", "lat"],
            get_color="color",
            pickable=True,
            opacity=0.9 if fill_mode else 0.8,
            stroked=False,
            filled=True,
            parameters={"depthTest": False}, 
            **radius_settings
        )

        # Centrar el mapa
        view_state = pdk.ViewState(
            latitude=df["lat"].mean(),
            longitude=df["lon"].mean(),
            zoom=4,
            pitch=0,
        )

        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": f"Variable ({data_col}): {{{data_col}}}\nLat: {{lat}}\nLon: {{lon}}"},
        )

        st.pydeck_chart(r)

        with st.expander(f"Estadísticas de {data_col}"):
            st.write(df[[data_col, "lat", "lon"]].describe())
    else:
        st.info("No hay datos para mostrar en esta selección.")

def main():
    st.title("🌦️ AWS Meteo Colab - STI Viewer")

    # Initialize API Client
    # Usamos st.session_state para no reinicializar la clase en cada rerun si no es necesario
    if 'api' not in st.session_state:
        st.session_state.api = MeteoAPI()
    
    api = st.session_state.api

    # Tabs for Data Source
    tab_api, tab_local = st.tabs(["📡 API Data (S3)", "📂 Local File (Testing)"])

    # --- TAB 1: API DATA ---
    with tab_api:
        col1, col2 = st.columns(2)
        
        # Obtener Runs
        runs = get_runs_cached(api)
        
        with col1:
            selected_run = st.selectbox("Select Run", runs) if runs else None

        with col2:
            if selected_run:
                steps = get_steps_cached(api, selected_run)
                selected_step = st.selectbox("Select Step (Horizon)", steps) if steps else None
            else:
                selected_step = None

        if selected_run and selected_step:
            st.info(f"Viewing Run: **{selected_run}** | Step: **{selected_step}**")
            
            with st.spinner("Fetching geospatial data from API..."):
                try:
                    # Usamos la función con caché
                    df = get_spatial_data_cached(api, selected_run, selected_step)
                    
                    # Asumimos que la API devuelve 'sti', si devuelve otra cosa, ajustar aquí
                    target_col = "sti" if "sti" in df.columns else df.columns[0] # Fallback a la primera columna
                    
                    render_map(df, data_col=target_col)
                except Exception as e:
                    st.error(f"Error procesando datos de la API: {e}")

        elif not runs:
             st.error("Could not connect to API or no runs found.")

    # --- TAB 2: LOCAL FILE ---
    with tab_local:
        st.markdown("### Visualizador de NetCDF Local")
        st.markdown("Sube un archivo `.nc` para visualizar cualquier variable que contenga.")

        uploaded_file = st.file_uploader("Arrastra tu archivo NetCDF aquí", type=["nc"])

        if uploaded_file is not None:
            import xarray as xr
            import tempfile
            import os

            # Guardar en temporal para que xarray pueda leerlo
            with tempfile.NamedTemporaryFile(delete=False, suffix=".nc") as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name

            try:
                with xr.open_dataset(tmp_path, engine="h5netcdf") as ds:
                    
                    # CORRECCIÓN 1: Usar .sizes en lugar de .dims para evitar el Warning
                    st.write("### Metadatos del Archivo")
                    with st.expander("Ver detalles del NetCDF"):
                        st.json({
                            "dimensiones": dict(ds.sizes),
                            "variables": list(ds.data_vars.keys()),
                            "coordenadas": list(ds.coords.keys())
                        })

                    # CORRECCIÓN 2: Selector de Variables Dinámico
                    avail_vars = list(ds.data_vars.keys())
                    if avail_vars:
                        selected_var = st.selectbox("Selecciona la variable a visualizar:", avail_vars, index=0)
                        
                        st.subheader(f"Visualizando: {selected_var}")

                        # Lógica de subset (toma el primer tiempo/paso si existen)
                        data_array = ds[selected_var]
                        if "time" in data_array.dims:
                            data_array = data_array.isel(time=0)
                        if "step" in data_array.dims:
                            data_array = data_array.isel(step=0)

                        # Convertir a DataFrame
                        df_local = data_array.to_dataframe().reset_index()

                        # Normalizar nombres de columnas comunes
                        df_local = df_local.rename(columns={
                            "latitude": "lat", 
                            "longitude": "lon",
                            "ylat": "lat",
                            "xlon": "lon"
                        })

                        if "lat" in df_local.columns and "lon" in df_local.columns:
                            # CORRECCIÓN 3: Pasar el nombre real de la variable a render_map
                            render_map(df_local, data_col=selected_var)
                        else:
                            st.error(f"No se encontraron coordenadas lat/lon válidas. Columnas detectadas: {df_local.columns.tolist()}")
                    else:
                        st.warning("El archivo no contiene variables de datos visualizables.")

            except Exception as e:
                st.error(f"Error leyendo el archivo: {e}")
            finally:
                # Limpieza
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

if __name__ == "__main__":
    main()