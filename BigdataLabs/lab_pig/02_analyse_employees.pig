-- Quel est le salaire moyen des employés dans chaque département ?

employees = LOAD 'input/employees.txt' USING PigStorage(',') 
  AS (id:int, fname:chararray, lname:chararray, dept_no:int, 
      region:chararray, salary:int, gender:chararray);

departments = LOAD 'input/department.txt' USING PigStorage(',') 
  AS (dept_no:int, dept_name:chararray);

salary_by_dept = GROUP employees BY dept_no;
avg_salary = FOREACH salary_by_dept 
  GENERATE group AS dept_no, AVG(employees.salary) AS avg_salary;
DUMP avg_salary;

--  Combien d'employés travaillent dans chaque département ?
emp_count_by_dept = GROUP employees BY dept_no;
emp_count = FOREACH emp_count_by_dept 
  GENERATE group AS dept_no, COUNT(employees) AS nb_employes;
DUMP emp_count;

-- Lister tous les employés avec leurs départements respectifs
emp_with_dept = JOIN employees BY dept_no, departments BY dept_no;
result3 = FOREACH emp_with_dept 
  GENERATE employees::id, employees::fname, employees::lname, 
           departments::dept_name;
DUMP result3;

-- Quels sont les employés ayant un salaire supérieur à 60 000 ?
high_salary = FILTER employees BY salary > 60000;
DUMP high_salary;



