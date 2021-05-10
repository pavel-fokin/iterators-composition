import pipelines

API_URL = "https://api.nasa.gov/neo/rest/v1/feed?start_date={}&api_key={}"


def main():
    pipe = pipelines.compose(
        pipelines.Load(
            url=API_URL, api_key="DEMO_KEY", start_date="2021-05-09"
        ),
        pipelines.Write(to_path="/tmp"),
        pipelines.Find(field="is_potentially_hazardous_asteroid"),
    )

    for each in pipe():
        print(each)


if __name__ == "__main__":
    main()
