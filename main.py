import uuid
import redis


def generate_short_url():
    return str(uuid.uuid4())[:6]  # 6666hg   https://abc.ru/6666hg


r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

while True:
    line = input().split(" ")  # insert http://mail.ru
    cmd, arg = None, None

    if len(line) == 2:
        cmd = line[0]
        arg = line[1]
    elif len(line) == 1:
        cmd = line[0]

    if cmd == 'insert' and arg is not None:
        print('Please, enter your e-mail below:')
        email = input()

        if r.hlen(email) == 0:
            short_url = generate_short_url()  # 4g8t7t
            r.hset(email, short_url, arg)
            print(short_url)

            r.hset("insertions", email, 1)
        else:
            isDuplicate = False

            for key in r.hgetall(email):
                value = r.hget(email, key)
                if arg == value:
                    isDuplicate = True
                    break

            if not isDuplicate:
                short_url = generate_short_url()
                r.hset(email, short_url, arg)
                print(short_url)

                r.hincrby("insertions", email, 1)
            else:
                print("(ERROR) link already added for", email)
    elif cmd == 'query' and arg is not None:
        print('Please, enter your e-mail below:')
        email = input()

        if arg is not None:
            long_string = r.hget(email, arg)

            if long_string is not None:
                print(long_string)
            else:
                print("(ERROR) no value found by key")
    elif cmd == 'stats':
        print("insertions by user:")
        for key in r.hgetall("insertions"):
            print(key + ": " + r.hget("insertions", key))
    elif cmd == 'exit':
        exit(0)
    else:
        if cmd in ('query', 'insert') and arg is None:
            print("Try using: " + cmd + " <arg>")
        else:
            print("Command '" + cmd + "' not found! Please, try again.")
