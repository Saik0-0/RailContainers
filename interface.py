from pathlib import Path
import streamlit as st
from combination_generator import CombinationGenerator

st.title('Генерация допустимых размещений контейнеров на ЖД платформы')
st.subheader('Загрузка файла')
uploaded_file = st.file_uploader("Upload a .bak file", type=['bak'])
if uploaded_file is not None:
    df = uploaded_file
    file_name = Path(uploaded_file.name).stem
    st.write("success")
st.subheader('Начать генерацию')
button = st.button('Начать генерацию', key='1')
if button:
    st.write('Началась генерация')
    cg = CombinationGenerator(file_name)
    cg.create()
    st.write('Генерация окончена')