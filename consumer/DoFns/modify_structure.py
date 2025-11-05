import apache_beam as beam

class ModifyStructure(beam.DoFn):
    def process(self, element):
        try:
            flat_record = { # Todo: Modify depending warehouse schema
                'document_id': element.get('document_id'),
                'video_id': element['data'].get('video_id'),
                'session_id': element['data'].get('session_id'),
                'watched_seconds': element['data'].get('watched_seconds'),
                'video_duration_seconds': element['data'].get('video_duration_seconds'),
                'watched_ratio': element['data'].get('watched_ratio'),
                'device_type': element['data'].get('device_type'),
                'quality': element['data'].get('quality'),
                'processed_at': element['data'].get('timestamp')
            }

            yield flat_record

        except Exception as e:
            print(f"Error modifying structure: {e}")