def A(fun):
    print(56)
    def printer():
        print("双11机器人5折开始")
        print('qwewe')
        fun()
        print("双11机器人5折结束")

    return printer


@A
def say_hello():
    print("您好")


@A
def goodbye():
    print("在家")


say_hello()