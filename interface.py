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

st.subheader('Введите номер сессии, для которой необходимо произвести расчёт')
sessionID = st.number_input("Сессия №", min_value=1, max_value=400, value=1)
st.subheader('Генерация размещений')
button = st.button('Начать генерацию', key='1')
if button:
    if uploaded_file is None:
        st.warning('Загрузите файл с базой данных!')
    else:
        st.write('Началась генерация ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        cg = CombinationGenerator(file_name, sessionID)
        result_file = cg.process_session()
        if result_file is None:
            st.warning('В базе данных нет сессии с данным номером')
        else:
            st.write('Генерация окончена ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            st.subheader("Экспорт результата")
            with open("result_session_" + str(sessionID) + ".json", 'rb') as f:
                st.download_button("Скачать файл", f, "result_session_" + str(sessionID) + ".json")
