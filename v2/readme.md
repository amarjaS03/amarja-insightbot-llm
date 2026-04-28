# Readme.md

1. File names must use lowercase_with_underscores (e.g., get_employee.py).
2. Class and interface names must follow PascalCase (e.g., class EmployeeDetails:).
3. Private variables inside a class must start with double underscores (e.g., __variableName).
4. Method names inside a class must start with a single underscore (e.g., _methodName).
5. Function names outside a class must follow lower_snake_case.
6. Folder names must be entirely lowercase.

employee_details.py
class EmployeeDetails:
    def __init__(self, name, age, salary):
        self.__name = name            # ✔ private variable (__variableName)
        self.__age = age
        self.__salary = salary

    def _calculate_bonus(self):        # ✔ method inside class starts with _
        return self.__salary * 0.10

    def _get_employee_summary(self):   # ✔ method inside class starts with _
        return {
            "name": self.__name,
            "age": self.__age,
            "salary": self.__salary,
            "bonus": self._calculate_bonus()
        }

def print_employee_report(employee: EmployeeDetails):   # ✔ lower_snake_case
    summary = employee._get_employee_summary()
    print("Employee Report:", summary)

📁 Folder Structure (following rule: folder names lowercase)
my_project/
    employees/
        employee_details.py
    reports/
        employee_report.py
