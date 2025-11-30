import subprocess

class SimpleDB:
    def __init__(self, db_file):
        # ./db 프로세스 실행 (stdin/stdout 파이프 연결)
        self.process = subprocess.Popen(
            ["./db", db_file],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=0 # Unbuffered
        )

    def execute(self, command):
        # 명령어 입력
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()
        
        # 결과 읽기 (간단한 예시를 위해 한 줄만 읽음)
        # 실제로는 "db >" 프롬프트가 나올 때까지 읽어야 함
        output = []
        while True:
            line = self.process.stdout.readline()
            if "db >" in line: # 프롬프트가 나오면 명령 완료로 간주
                break
            if line:
                output.append(line.strip())
        return output

    def close(self):
        self.execute(".exit")
        self.process.wait()

# 사용 예시
db = SimpleDB("my.db")
print(db.execute("insert 10 user10 test@example.com"))
print(db.execute("select"))
db.close()
