with open("C:\\lct-dm-p\\lct-mn-auto\\python_classes\\mini_project\\source_folders\\givers.csv", "r") as file:
    content = file.readlines() # Read first line of the file
    print(len(content[4].split(",")[4].strip()))