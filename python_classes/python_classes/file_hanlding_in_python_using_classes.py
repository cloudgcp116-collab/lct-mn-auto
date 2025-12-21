class FileHandler:
    def __init__(self, file_path):
        self.file_path = file_path

    # just print the conetent of the file
    def file_reading(self):
        with open(self.file_path, "r") as file:
            content = file.read()
            print(content)
            return content

    # read and write only the odd rows to a new file
    def file_read_odd_lines(self, output_path):
        with open(self.file_path, "r") as file:
            lines = file.readlines()
        
        odd_lines = [line for index, line in enumerate(lines) if index % 2 != 0]
        
        with open(output_path, "w") as file:
            file.writelines(odd_lines)
    # read and write only the even rows to a new file
    def file_read_even_lines(self, output_path):
        with open(self.file_path, "r") as file:
            lines = file.readlines()
        
        even_lines = [line for index, line in enumerate(lines) if index % 2 == 0]
        
        with open(output_path, "w") as file:
            file.writelines(even_lines)  
    

    
file_handler = FileHandler(r"C:\\lct-dm-p\\lct-mn-auto\\python_classes\\mini_project\\source_folders\\givers.csv")
file_handler.file_reading() 
file_handler.file_read_odd_lines(r"C:\\lct-dm-p\\lct-mn-auto\\python_classes\\mini_project\\source_folders\\givers_odd.csv")
file_handler.file_read_even_lines(r"C:\\lct-dm-p\\lct-mn-auto\\python_classes\\mini_project\\source_folders\\givers_even.csv")  

