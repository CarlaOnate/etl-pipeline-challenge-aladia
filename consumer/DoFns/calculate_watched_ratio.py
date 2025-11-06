import apache_beam as beam

class CalculateWatchedRatio(beam.DoFn):
    def process(self, element):
        try:
            data = element.copy()
            video_data = data.get('data', {})

            watched_seconds = video_data.get('watched_seconds', 0)
            duration = video_data.get('video_duration_seconds', 1)

            watched_ratio = watched_seconds / duration if duration > 0 else 0
            video_data['watched_ratio'] = round(watched_ratio, 4)

            data['data'] = video_data
            yield data

        except Exception as e:
            print(f"Error calculating ratio: {e}")