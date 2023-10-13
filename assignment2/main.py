import os 
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import data_handler 
import time

def main():
    while True:
        print('Henter data')

        # Starter sparksession.builder
        data_handler.start_spark_session()

        # Hent data fra API
        data = data_handler.extract_data()

        # Transformér data ved hjælp af PySpark
        transformed_data = data_handler.transform_data(data)

        # gennemgår transformered_data
        for row in transformed_data:
            year = row['Year']  # Opdater til 'Year' i stedet for 'Month'
            suppliers = row['TotalActiveSuppliers']
            print(f'År: {year} - sum af Total Active Suppliers: {suppliers}')

        data_handler.load_data(transformed_data)

        print("Data gemt i csv")

        data_handler.print_graph(transformed_data)

        print('Graf printet')

        # Stop SparkSession.builder
        data_handler.stop_spark_session()

        print('Slukket for spark')

        # Vent i 1 time (3600 sekunder)
        print('Henter data om 1 time')
        time.sleep(3600)

        

if __name__ == "__main__":
    main()
