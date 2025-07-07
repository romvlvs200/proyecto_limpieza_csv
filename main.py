import streamlit as st

st.set_page_config(
    page_title="Data Analysis Suite",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Suite de Análisis de Datos")
st.sidebar.success("Selecciona una página arriba.")

st.markdown("""
## Bienvenido a tu Suite de Análisis de Datos

### Páginas disponibles:
- **Limpieza CSV**: Limpia y procesa archivos CSV
- **Transformación**: Transforma y analiza datos

Selecciona una página en la barra lateral para comenzar.
""")