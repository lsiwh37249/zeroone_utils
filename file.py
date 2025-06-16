def save_file(file_path, data):
    """
    데이터를 주어진 파일 경로에 저장합니다.
    
    Parameters:
    - file_path (str): 저장할 파일의 경로
    - data (str): 저장할 문자열 데이터
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(data)
        print(f"파일이 저장되었습니다: {file_path}")
    except Exception as e:
        print(f"파일 저장 중 오류 발생: {e}")

def load_file(file_path):
    """
    주어진 파일 경로에서 데이터를 불러옵니다.

    Parameters:
    - file_path (str): 불러올 파일의 경로

    Returns:
    - str: 파일의 내용 (읽기 성공 시)
    - None: 오류 발생 시
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = f.read()
        print(f"파일이 불러와졌습니다: {file_path}")
        return data
    except Exception as e:
        print(f"파일 불러오기 중 오류 발생: {e}")
        return None
