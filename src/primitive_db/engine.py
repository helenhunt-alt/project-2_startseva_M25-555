import prompt


def welcome():
    print("\nПервая попытка запустить проект!\n")
    print("***")
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")
    
    while True:
        command = prompt.string("Введите команду: ").strip().lower()
        
        if command == "exit":
            print("Выход из программы...")
            break
        elif command == "help":
            print("<command> exit - выйти из программы")
            print("<command> help - справочная информация")
        elif command == "":
            continue
        else:
            print(f"Команда '{command}' не распознана. Введите help для справки.")