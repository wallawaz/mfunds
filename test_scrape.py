from scraper import MFScraper
from utils import cache_path

limit = ["pnc funds"]
datasource = "yahoo"
cache_name = cache_path()

if __name__ == "__main__":
    mf_scraper = MFScraper("yahoo", cache_name, 7, 5)

    mf_scraper.run_all(limit=limit)
    import ipdb; ipdb.set_trace()
    print(mf_scraper.fund_families)
