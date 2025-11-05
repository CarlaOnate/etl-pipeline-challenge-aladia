import apache_beam as beam

class WriteToDataWarehouse(beam.DoFn):
    def setup(self):
        """Initialize database connection once per worker"""
        print("Setting up data warehouse connection")

    def process(self, element):
        try:
            # Write to your data warehouse
            print(f"Writing to warehouse: {element}")
            yield element

        except Exception as e:
            print(f"Error writing to warehouse: {e}")