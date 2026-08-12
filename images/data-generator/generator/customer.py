from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
import random
import names
from generator.base import BaseDatabase, BaseEntity


occupations = [
    "Software Engineer",
    "Data Engineer",
    "Data Scientist",
    "DevOps Engineer",
    "Systems Administrator",
    "IT Support Specialist",
    "Cybersecurity Analyst",
    "Product Manager",
    "Project Manager",
    "Business Analyst",
    "Accountant",
    "Financial Analyst",
    "Investment Analyst",
    "Bank Teller",
    "Loan Officer",
    "Compliance Officer",
    "Auditor",
    "Lawyer",
    "Paralegal",
    "Doctor",
    "Nurse",
    "Pharmacist",
    "Dentist",
    "Physiotherapist",
    "Psychologist",
    "Teacher",
    "Professor",
    "Researcher",
    "Student",
    "School Administrator",
    "Civil Engineer",
    "Mechanical Engineer",
    "Electrical Engineer",
    "Architect",
    "Surveyor",
    "Construction Worker",
    "Electrician",
    "Plumber",
    "Carpenter",
    "Mechanic",
    "Factory Worker",
    "Machine Operator",
    "Quality Inspector",
    "Truck Driver",
    "Delivery Driver",
    "Pilot",
    "Flight Attendant",
    "Train Driver",
    "Logistics Coordinator",
    "Warehouse Operative",
    "Retail Assistant",
    "Store Manager",
    "Cashier",
    "Sales Representative",
    "Sales Manager",
    "Marketing Manager",
    "Digital Marketer",
    "Graphic Designer",
    "UX Designer",
    "Customer Service Representative",
    "Call Centre Agent",
    "Chef",
    "Cook",
    "Waiter",
    "Bartender",
    "Hotel Manager",
    "Receptionist",
    "Travel Agent",
    "Farmer",
    "Agricultural Worker",
    "Veterinarian",
    "Real Estate Agent",
    "Property Manager",
    "Journalist",
    "Photographer",
    "Videographer",
    "Writer",
    "Musician",
    "Actor",
    "Social Worker",
    "Police Officer",
    "Firefighter",
    "Military Personnel",
    "Government Officer",
    "HR Manager",
    "Recruiter",
    "Consultant",
    "Entrepreneur",
    "Business Owner",
    "Self-Employed",
    "Freelancer",
    "Unemployed",
    "Retired",
]


def get_random_gender():
    return random.choice([Gender.MALE, Gender.FEMALE])


class Gender(StrEnum):
    MALE = "male"
    FEMALE = "female"


@dataclass
class Customer(BaseEntity):
    id: str
    created_at: datetime
    firstname: str
    lastname: str
    date_of_birth: date
    occupation: str
    address: str
    gender: str


class CustomerDatabase(BaseDatabase):
    def __init__(self):
        super().__init__(Customer)
        self.dob_start_ordinal = date(1970, 1, 1).toordinal()
        self.dob_end_ordinal = date(2010, 12, 31).toordinal()

    def customer_created(self) -> Customer:
        customer = Customer(
            id=self._get_next_id(),
            created_at=datetime.now(),
            firstname=names.get_first_name(),
            lastname=names.get_last_name(),
            date_of_birth=date.fromordinal(
                random.randint(self.dob_start_ordinal, self.dob_end_ordinal)
            ),
            occupation=random.choice(occupations),
            address=self._faker.address(),
            gender=get_random_gender(),
        )
        self._insert(customer)
        return customer

    def customer_details_updated(self) -> Customer:
        # select customer
        idx = random.randint(0, len(self._database) - 1)
        customer = self._database[idx]

        # choose random field to update
        fields = [
            "firstname",
            "lastname",
            "occupation",
            "address",
        ]
        field = random.choice(fields)

        match field:
            case "firstname":
                customer.firstname = self._faker.first_name()
            case "lastname":
                customer.lastname = self._faker.last_name()
            case "occupation":
                customer.occupation = random.choice(occupations)
            case "address":
                customer.address = self._faker.address()

        # update customer list and return customer
        self._update_persistence(customer)
        self._database[idx] = customer
        return customer
