import requests
from dotenv import load_dotenv
from data.logger import Logger

load_dotenv()


class PlatformApi:
    BASE_URL = 'https://hdm-{platform}.roadly.cc/'
    HEADERS = {
        'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
    }

    def __init__(self, platform='dev'):
        self.platform = platform
        self.logger = Logger('hdm.log')
        self.session = self.authorization()
        

    def authorization(self) -> requests.Session or None:
        url = self.BASE_URL.format(platform=self.platform) + 'api/login'
        headers = {
        'sec-ch-ua':
        '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
        'sec-ch-ua-platform': '"macOS"',
        'Referer': f'https://hdm-{self.platform}.roadly.cc/signin',
        'sec-ch-ua-mobile': '?0',
        'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        }
        json_data = {
        'email': 'hdmap@road.ly',
        'password': '090Roadly!',
    }
        session = requests.Session()
        response = session.post(url, headers=headers, json=json_data)
        #print(response.status_code)
        return session
    
    def del_pipeline(self, pipeline_id: str) -> bool or None:
        url = self.BASE_URL.format(platform=self.platform) + f'pci-api/v1/pipeline/{pipeline_id}'
        try:
            response = self.session.delete(url)
            #print(response.status_code)
            #print(response.text)
            if response.status_code == 200:
                self.logger.log(f"Successfully deleted pipeline: {pipeline_id}")
                return True
        except Exception as e:
            self.logger.log(f"Failed to delete pipeline: {pipeline_id} - {e}")
            return False
    
    def get_pipelines(self, pipeline_id: str) -> dict or None:
        url = self.BASE_URL.format(platform=self.platform) + f'api/get_pipelines/{pipeline_id}'
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                data = response.json()[0]
                data['platform'] = self.platform
                self.logger.log(f"Successfully retrieved pipeline: {pipeline_id}")
                return data
        except Exception as e:
            self.logger.log(f"Failed to retrieve pipeline: {pipeline_id} - {e}")
            return None
        
    def get_session(self, session_id: str) -> dict or None:
        url = self.BASE_URL.format(platform=self.platform) + f'api/get_session/{session_id}'
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                data = response.json()
                data['platform'] = self.platform
                self.logger.log(f"Successfully retrieved session: {session_id}")
                return data
        except Exception as e:
            self.logger.log(f"Failed to retrieve session: {session_id} - {e}")
            return None
        
    def get_sessions(self, folder_prefix: str, folder_name: str, limit: int=100) -> dict or None:
        url = self.BASE_URL.format(platform=self.platform) + 'api/get_sessions'
        params = {
                'folder_prefix': folder_prefix,
                'folder_name': folder_name,
                'limit': limit
            }
        try:
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                data['platform'] = self.platform
                self.logger.log(f"Successfully retrieved sessions for folder: {folder_name}")
                return data
        except Exception as e:
            self.logger.log(f"Failed to retrieve sessions for folder: {folder_name} - {e}")
            return None
        
    def get_list_pipelines(self, type, **kwargs):
        url = self.BASE_URL.format(platform=self.platform) + 'api/get_pipelines'
        params = {'types': type, **kwargs}
        try:
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                data['platform'] = self.platform
                self.logger.log(f"Successfully retrieved pipelines")
                return data
        except Exception as e:
            self.logger.log(f"Failed to retrieve sessions pipelines - {e}")
            return None
        

    def del_area(self, polygon):
        url = self.BASE_URL.format(platform=self.platform) + f'pci-api/v1/pipeline/polygon/{polygon}'
        #params = {'polygon': polygon}
        try:
            response = self.session.delete(url)
            print(response.status_code)
            print(response.text)
            if response.status_code == 200:
                self.logger.log(f"Successfully deleted area: {polygon}")
                return True
        except Exception as e:
            self.logger.log(f"Failed to delete area: {polygon} - {e}")
            return False