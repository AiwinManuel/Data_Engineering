import pandas as pd
from faker import Faker
import random
import uuid

faker = Faker()

# Setting random seed for reproducibility 
Faker.seed(45)
random.seed(45)


# Generating UUIDs 
def generate_uuid():
    return str(uuid.uuid4())


# Defining cities by country
cities_by_country = {
    "India": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Pune"],
    "USA": ["New York", "Los Angeles", "Chicago", "Houston", "San Francisco", "Boston", "Seattle"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa", "Edmonton", "Winnipeg"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra", "Hobart"]
}

# Generating Enployees Table

def generate_employees(num_employees):
    employees=[]
    for _ in range(num_employees):
        country = random.choice(list(cities_by_country.keys())) 
        employees.append({
            'EmployeeID': generate_uuid(),
            'FirstName': faker.first_name(),
            'LastName': faker.last_name(),
            "Gender": random.choice(["Male", "Female", "Other"]),
            "DateOfBirth": faker.date_of_birth(minimum_age=22, maximum_age=60),
            "HireDate": faker.date_between(start_date='-10y', end_date='today'),
            "DepartmentID": random.randint(1, 10),
            "PositionID": random.randint(1, 20),
            "Salary": random.randint(40000, 120000),
            "PerformanceScore": random.randint(1, 10),
            "Email": faker.email(),
            "PhoneNumber": faker.phone_number(),
            "ManagerID": random.choice([None] + [str(uuid.uuid4()) for _ in range(10)]),
            "EmploymentType": random.choice(["Full-Time", "Part-Time", "Contract"]),
            "Location": country,  
            
        })
    return pd.DataFrame(employees)


# Generating Departments Table
def generate_departments(num_departments):
    departments = []
    for dept_id in range(1, num_departments + 1):
        departments.append({
            "DepartmentID": dept_id,
            "DepartmentName": faker.bs().capitalize(),
            "ManagerID": generate_uuid(),
            "Budget": random.randint(100000, 500000),
            "Location": faker.city(),
            "EstablishedDate": faker.date_between(start_date='-20y', end_date='-10y'),
        })
    return pd.DataFrame(departments)


# Generating Positions Table
def generate_positions(num_positions):
    positions = []
    for pos_id in range(1, num_positions + 1):
        positions.append({
            "PositionID": pos_id,
            "PositionTitle": faker.job(),
            "MinSalary": random.randint(30000, 50000),
            "MaxSalary": random.randint(70000, 120000),
            "DepartmentID": random.randint(1, 10),
            "SeniorityLevel": random.choice(["Junior", "Mid", "Senior"]),
        })
    return pd.DataFrame(positions)


# Generating Salary History Table
def generate_salary_history(num_records, employee_ids=None):
    salary_history = []
    for _ in range(num_records):
        emp_id = random.choice(employee_ids)
        prev_salary = random.randint(40000, 100000)
        updated_salary = prev_salary + random.randint(1000, 10000)
        salary_history.append({
            "SalaryHistoryID": str(uuid.uuid4()),
            "EmployeeID": emp_id,
            "PreviousSalary": prev_salary,
            "UpdatedSalary": updated_salary,
            "EffectiveDate": faker.date_between(start_date='-5y', end_date='today'),
            "ChangeReason": random.choice(["Promotion", "Annual Raise", "Adjustment"]),
        })
    return pd.DataFrame(salary_history)


# Generating Performance Reviews Table
def generate_performance_reviews(num_reviews=200, employee_ids=None):
    reviews = []
    for _ in range(num_reviews):
        reviews.append({
            "ReviewID": generate_uuid(),
            "EmployeeID": random.choice(employee_ids),
            "ReviewDate": faker.date_between(start_date='-3y', end_date='today'),
            "ReviewerID": random.choice(employee_ids),
            "Score": random.randint(1, 10),
            "Comments": faker.sentence(),
            "PromotionRecommendation": random.choice(["Yes", "No"]),
        })
    return pd.DataFrame(reviews)



# Generating Leave Records Table
def generate_leave_records(num_leaves=200, employee_ids=None):
    leave_records = []
    for _ in range(num_leaves):
        start_date = faker.date_between(start_date='-2y', end_date='today')
        end_date = faker.date_between(start_date=start_date, end_date=start_date + pd.Timedelta(days=10))
        leave_records.append({
            "LeaveID": generate_uuid(),
            "EmployeeID": random.choice(employee_ids),
            "LeaveType": random.choice(["Sick Leave", "Vacation", "Unpaid Leave"]),
            "StartDate": start_date,
            "EndDate": end_date,
            "ApprovalStatus": random.choice(["Approved", "Pending", "Denied"]),
        })
    return pd.DataFrame(leave_records)



# Generating Attendance Records Table
def generate_attendance(num_records=1000, employee_ids=None):
    attendance = []
    for _ in range(num_records):
        date = faker.date_between(start_date='-1y', end_date='today')
        check_in = faker.time(pattern='%H:%M:%S', end_datetime=None)
        check_out = faker.time(pattern='%H:%M:%S', end_datetime=None)
        attendance.append({
            "AttendanceID": generate_uuid(),
            "EmployeeID": random.choice(employee_ids),
            "Date": date,
            "CheckInTime": check_in,
            "CheckOutTime": check_out,
            "HoursWorked": random.randint(4, 8),
            "WorkLocation": random.choice(["Office", "Remote"]),
        })
    return pd.DataFrame(attendance)


# Generating Training Data Table
def generate_training_data(num_trainings=100, employee_ids=None):
    training_data = []
    for _ in range(num_trainings):
        training_data.append({
            "TrainingID": generate_uuid(),
            "EmployeeID": random.choice(employee_ids),
            "TrainingTitle": faker.catch_phrase(),
            "TrainingDate": faker.date_between(start_date='-2y', end_date='today'),
            "DurationHours": random.randint(4, 40),
            "Cost": random.randint(500, 5000),
            "Certification": random.choice(["Yes", "No"]),
        })
    return pd.DataFrame(training_data)


# Generating Payroll Data Table
def generate_payroll_data(num_records=200, employee_ids=None):
    payroll = []
    for _ in range(num_records):
        payroll.append({
            "PayrollID": generate_uuid(),
            "EmployeeID": random.choice(employee_ids),
            "PayDate": faker.date_between(start_date='-1y', end_date='today'),
            "GrossPay": random.randint(4000, 8000),
            "Deductions": random.randint(500, 2000),
            "NetPay": random.randint(3000, 6000),
        })
    return pd.DataFrame(payroll)


# Generating Benefits Data Table
def generate_benefits_data(num_records=100, employee_ids=None):
    benefits = []
    for _ in range(num_records):
        benefits.append({
            "BenefitID": generate_uuid(),
            "EmployeeID": random.choice(employee_ids),
            "BenefitType": random.choice(["Health Insurance", "Retirement Plan", "Gym Membership"]),
            "StartDate": faker.date_between(start_date='-5y', end_date='-1y'),
            "EndDate": faker.date_between(start_date='-1y', end_date='today'),
            "CostToCompany": random.randint(1000, 5000),
            "ContributionByEmployee": random.randint(200, 1000),
        })
    return pd.DataFrame(benefits)


# Generating Resignations Data Table
def generate_resignations(num_records=50, employee_ids=None):
    resignations = []
    for _ in range(num_records):
        resignations.append({
            "TerminationID": generate_uuid(),
            "EmployeeID": random.choice(employee_ids),
            "LastWorkingDate": faker.date_between(start_date='-1y', end_date='today'),
            "Reason": random.choice(["Voluntary Resignation", "Layoff", "Retirement"]),
            "Feedback": faker.sentence(),
            "SeverancePackage": random.randint(5000, 20000),
        })
    return pd.DataFrame(resignations)


# Generating Projects Data
def generate_projects(num_projects=50):
    projects = []
    for _ in range(num_projects):
        projects.append({
            "ProjectID": generate_uuid(),
            "ProjectName": faker.catch_phrase(),
            "StartDate": faker.date_between(start_date='-5y', end_date='-1y'),
            "EndDate": faker.date_between(start_date='-1y', end_date='today'),
            "Budget": random.randint(10000, 500000),
            "Status": random.choice(["Active", "Completed", "On Hold"]),
        })
    return pd.DataFrame(projects)

# Generating Project Assignments Data
def generate_project_assignments(num_assignments=200, employee_ids=None, project_ids=None):
    assignments = []
    for _ in range(num_assignments):
        assignments.append({
            "AssignmentID": generate_uuid(),
            "ProjectID": random.choice(project_ids),
            "EmployeeID": random.choice(employee_ids),
            "StartDate": faker.date_between(start_date='-2y', end_date='-1y'),
            "EndDate": faker.date_between(start_date='-1y', end_date='today'),
            "Role": random.choice(["Lead", "Contributor", "Reviewer"]),
            "HoursAllocated": random.randint(10, 100),
        })
    return pd.DataFrame(assignments)

# Generating Employee Projects Data
num_employees = 100
employees_df = generate_employees(num_employees)
departments_df = generate_departments(10)
positions_df = generate_positions(20)
salary_history_df = generate_salary_history(200, employees_df["EmployeeID"].tolist())
performance_reviews_df = generate_performance_reviews(200, employees_df["EmployeeID"].tolist())
leave_records_df = generate_leave_records(200, employees_df["EmployeeID"].tolist())
attendance_df = generate_attendance(1000, employees_df["EmployeeID"].tolist())
training_df = generate_training_data(100, employees_df["EmployeeID"].tolist())
payroll_df = generate_payroll_data(200, employees_df["EmployeeID"].tolist())
benefits_df = generate_benefits_data(100, employees_df["EmployeeID"].tolist())
resignations_df = generate_resignations(50, employees_df["EmployeeID"].tolist())
projects_df = generate_projects(50)
project_assignments_df = generate_project_assignments(200, employees_df["EmployeeID"].tolist(), projects_df["ProjectID"].tolist())


# # Saving to CSV files
employees_df.to_csv("employees.csv", index=False)
departments_df.to_csv("departments.csv", index=False)
positions_df.to_csv("positions.csv", index=False)
salary_history_df.to_csv("salary_history.csv", index=False)
performance_reviews_df.to_csv("performance_reviews.csv", index=False)
leave_records_df.to_csv("leave_records.csv", index=False)
attendance_df.to_csv("attendance.csv", index=False)
training_df.to_csv("training.csv", index=False)
payroll_df.to_csv("payroll.csv", index=False)
benefits_df.to_csv("benefits.csv", index=False)
resignations_df.to_csv("resignations.csv", index=False)
projects_df.to_csv("projects.csv", index=False)
project_assignments_df.to_csv("project_assignments.csv", index=False)

print("Data generated and saved as CSV files.")
