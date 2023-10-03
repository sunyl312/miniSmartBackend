import json


def UserPackage(name):
    """_summary_

    Args:
        name (_type_): 通过输入想输入的配置名称，如ftp90

    Returns:
        _type_: 返回这个名称的所有信息，可与通过key的方式获取其相关的name等
    """
    with open('./config.json') as json_file:
        config = json.load(json_file)
    return config[name]


if __name__ == "__main__":

    # print(UserPackage("ftp21")["login"]["password"])
    print(UserPackage("gsadb"))
