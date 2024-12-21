from pathlib import Path
import streamlit as st
from combination_generator import CombinationGenerator
import datetime


st.title('Генерация допустимых размещений контейнеров на ЖД платформы')
st.subheader('Загрузка файла')
uploaded_file = st.file_uploader("Upload a .bak file", type=['bak'])
if uploaded_file is not None:
    df = uploaded_file
    file_name = Path(uploaded_file.name).stem
    st.write("Файл загружен")
st.subheader('Введите номер сессии, для которой необходимо произвести расчёт')

st.subheader('Генерация размещений')
button = st.button('Начать генерацию', key='1')
if button:
    st.write('Началась генерация ' + str(datetime.datetime.now()))
    cg = CombinationGenerator(file_name, 16)
    result_file = cg.create()
    st.write('Генерация окончена ' + str(datetime.datetime.now()))

st.subheader('Экспорт результата')
