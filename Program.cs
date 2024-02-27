using System;
using System.Text.RegularExpressions;
using Npgsql;

namespace RealEstateDatabase
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = "Host=localhost;Port=5432;Username=postgres;Password=123;Database=postgres";
            string districtName = "South";
            double minCost = 15000;
            double maxCost = 300000;

            using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
            {
                connection.Open();


                //2.1
                string sqlQuery = @"SELECT Address, Area, Floor
                                    FROM RealEstateObject
                                    JOIN Districts ON RealEstateObject.DistrictCode = Districts.DistrictCode
                                    WHERE Districts.DistrictName = @DistrictName AND RealEstateObject.Cost >= @minCost AND RealEstateObject.Cost <= @MaxCost
                                    ORDER BY RealEstateObject.Cost DESC";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery, connection))
                {
                    command.Parameters.AddWithValue("DistrictName", districtName);
                    command.Parameters.AddWithValue("MinCost", minCost);
                    command.Parameters.AddWithValue("MaxCost", maxCost);

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Адрес\t\tПлощадь\tЭтаж");

                        while (reader.Read())
                        {
                            string address = reader.GetString(0);
                            double area = reader.GetDouble(1);
                            int floor = reader.GetInt32(2);

                            Console.WriteLine($"{address}\t{area}\t{floor}");
                        }
                    }
                }

                Console.WriteLine();

                // 2.2
                int roomCount = 4;
                string sqlQuery1 = @"SELECT LastName, FirstName, Patronymic
                                    FROM Realtor R
                                    JOIN Sales S ON R.RealtorCode = S.RealtorCode
                                    JOIN RealEstateObject REO ON S.ObjectCode = REO.ObjectCode
                                    WHERE REO.RoomCount = @roomCount";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery1, connection))
                {
                    command.Parameters.AddWithValue("RoomCount", roomCount);
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Фамилия\tИмя\tОтчество");

                        while (reader.Read())
                        {
                            string lastName = reader.GetString(0);
                            string firstName = reader.GetString(1);
                            string middleName = reader.GetString(2);

                            Console.WriteLine($"{lastName}\t{firstName}\t{middleName}");
                        }
                    }
                }
                Console.WriteLine();

                //2.3
                string sqlQuery3 = @"SELECT SUM(REO.Cost)
                                    FROM RealEstateObject REO
                                    JOIN Districts D ON REO.DistrictCode = D.DistrictCode
                                    WHERE REO.RoomCount = @RoomCount AND D.DistrictName = @DistrictName";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery3, connection))
                {
                    command.Parameters.AddWithValue("@RoomCount", roomCount);
                    command.Parameters.AddWithValue("@DistrictName", districtName);

                    object result = command.ExecuteScalar();

                    if (result != null && result != DBNull.Value)
                    {
                        double totalCost = Convert.ToDouble(result);
                        Console.WriteLine($"Общая стоимость всех двухкомнатных объектов недвижимости в районе '{districtName}': {totalCost}");
                    }
                    else
                    {
                        Console.WriteLine("Данные отсутствуют.");
                    }
                }

                Console.WriteLine();

                //2.4
                string realtorName = "Smith";
                string sqlQuery4 = @"SELECT MAX(REO.Cost), MIN(REO.Cost)
                                    FROM RealEstateObject REO
                                    JOIN Sales S ON REO.ObjectCode = S.ObjectCode
                                    JOIN Realtor R ON S.RealtorCode = R.RealtorCode
                                    WHERE R.LastName = @RealtorName";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery4, connection))
                {
                    command.Parameters.AddWithValue("@RealtorName", realtorName);

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            double maxCost1 = reader.GetDouble(0);
                            double minCost1 = reader.GetDouble(1);

                            Console.WriteLine($"Максимальная стоимость объекта недвижимости, проданного риэлтором '{realtorName}': {maxCost1}");
                            Console.WriteLine($"Минимальная стоимость объекта недвижимости, проданного риэлтором '{realtorName}': {minCost1}");
                        }
                        else
                        {
                            Console.WriteLine("Данные отсутствуют.");
                        }
                    }
                }
                Console.WriteLine();

                //2.5
                string propertyType = "Apartment";
                string criterion = "Location";
                string sqlQuery5 = @"SELECT AVG(EvaluationValue)
                                    FROM Evaluations E
                                    JOIN RealEstateObject REO ON E.ObjectCode = REO.ObjectCode
                                    JOIN Sales S ON REO.ObjectCode = S.ObjectCode
                                    JOIN Realtor R ON S.RealtorCode = R.RealtorCode
                                    JOIN Type PT ON REO.TypeCode = PT.TypeCode
                                    JOIN EvaluationCriteria EC ON E.EvaluationCode = EC.CriteriaCode
                                    WHERE PT.TypeName = @PropertyType 
                                    AND R.LastName = @RealtorName 
                                    AND EC.CriteriaName = @Criterion";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery5, connection))
                {
                    command.Parameters.AddWithValue("@PropertyType", propertyType);
                    command.Parameters.AddWithValue("@RealtorName", realtorName);
                    command.Parameters.AddWithValue("@Criterion", criterion);

                    object result = command.ExecuteScalar();

                    if (result != null && result != DBNull.Value)
                    {
                        double averageEvaluation = Convert.ToDouble(result);
                        Console.WriteLine($"Средняя оценка апартаментов по критерию 'Безопасность', проданных риэлтором '{realtorName}': {averageEvaluation}");
                    }
                    else
                    {
                        Console.WriteLine("Данные отсутствуют.");
                    }
                }
                Console.WriteLine();

                //2.6
                int floor6 = 2;
                string sqlQuery6 = @"SELECT D.DistrictName, COUNT(*)
                                    FROM RealEstateObject REO
                                    JOIN Districts D ON REO.DistrictCode = D.DistrictCode
                                    WHERE REO.Floor = @Floor
                                    GROUP BY D.DistrictName";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery6, connection))
                {
                    command.Parameters.AddWithValue("@Floor", floor6);

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Название района\tКоличество объектов недвижимости");

                        while (reader.Read())
                        {
                            string districtName6 = reader.GetString(0);
                            int propertyCount = reader.GetInt32(1);

                            Console.WriteLine($"{districtName6}\t\t{propertyCount}");
                        }
                    }
                }
                Console.WriteLine();

                //2.7
                string sqlQuery7 = @"SELECT R.LastName, COUNT(*)
                                    FROM Sales S
                                    JOIN Realtor R ON S.RealtorCode = R.RealtorCode
                                    JOIN RealEstateObject REO ON S.ObjectCode = REO.ObjectCode
                                    JOIN Type PT ON REO.TypeCode = PT.TypeCode
                                    WHERE PT.TypeName = @PropertyType
                                    GROUP BY R.LastName";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery7, connection))
                {
                    command.Parameters.AddWithValue("@PropertyType", propertyType);

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Фамилия риэлтора\tКоличество продаж");

                        while (reader.Read())
                        {
                            string lastName = reader.GetString(0);
                            int salesCount = reader.GetInt32(1);

                            Console.WriteLine($"{lastName}\t\t\t{salesCount}");
                        }
                    }

                }
                Console.WriteLine();

                //2.8
                string sqlQuery8 = @"SELECT DistrictName, Address, Cost, Floor
                                    FROM (
                                        SELECT D.DistrictName, REO.Address, REO.Cost, REO.Floor,
                                               ROW_NUMBER() OVER (PARTITION BY D.DistrictName ORDER BY REO.Cost DESC, REO.Floor ASC) AS row_num
                                        FROM RealEstateObject REO
                                        JOIN Districts D ON REO.DistrictCode = D.DistrictCode
                                    ) AS subquery
                                    WHERE row_num <= 3";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery8, connection))
                {
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Название района\tАдрес\t\tСтоимость\tЭтаж");

                        while (reader.Read())
                        {
                            string districtName8 = reader.GetString(0);
                            string address = reader.GetString(1);
                            double cost = reader.GetDouble(2);
                            int floor8 = reader.GetInt32(3);

                            Console.WriteLine($"{districtName8}\t\t{address}\t{cost}\t\t{floor8}");
                        }
                    }

                }
                Console.WriteLine();
                //2.9
                string sqlQuery9 = @"SELECT EXTRACT(YEAR FROM S.SaleDate) AS sale_year, COUNT(*) AS sale_count
                                    FROM Sales S
                                    JOIN Realtor R ON S.RealtorCode = R.RealtorCode
                                    WHERE R.LastName = @RealtorLastName
                                    GROUP BY sale_year
                                    HAVING COUNT(*) > 2";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery9, connection))
                {
                    command.Parameters.AddWithValue("@RealtorLastName", realtorName);

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine($"Года, в которых риэлтор {realtorName} продал больше двух объектов недвижимости:");

                        while (reader.Read())
                        {
                            int year = reader.GetInt32(0);
                            int saleCount = reader.GetInt32(1);

                            Console.WriteLine($"Год: {year}, Количество продаж: {saleCount}");
                        }
                    }
                }
                Console.WriteLine();

                //2.10
                string sqlQuery10 = @"SELECT EXTRACT(YEAR FROM SaleDate) AS sale_year, COUNT(*) AS sale_count
                                    FROM Sales
                                    GROUP BY sale_year
                                    HAVING COUNT(*) BETWEEN 2 AND 3";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery10, connection))
                {
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Года, в которых было размещено от 2 до 3 объектов недвижимости:");

                        while (reader.Read())
                        {
                            int year = reader.GetInt32(0);
                            int saleCount = reader.GetInt32(1);

                            Console.WriteLine($"Год: {year}, Количество продаж: {saleCount}");
                        }
                    }
                }
                Console.WriteLine();
                //2.11
                string sqlQuery11 = @"SELECT REO.Address, D.DistrictName
                                    FROM RealEstateObject REO
                                    JOIN Districts D ON REO.DistrictCode = D.DistrictCode
                                    JOIN Sales S ON REO.ObjectCode = S.ObjectCode
                                    WHERE REO.Cost / S.SaleCost BETWEEN 0.8 AND 1.2";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery11, connection))
                {
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Адрес\t\tНазвание района");

                        while (reader.Read())
                        {
                            string address11 = reader.GetString(0);
                            string districtName11 = reader.GetString(1);

                            Console.WriteLine($"{address11}\t{districtName11}");
                        }
                    }
                }
                Console.WriteLine();

                //2.12 У меня равны
                string sqlQuery12 = @"SELECT REO.Address
                                    FROM RealEstateObject REO
                                    JOIN Districts D ON REO.DistrictCode = D.DistrictCode
                                    WHERE (REO.Cost / REO.Area) < (
                                        SELECT SUM(REO2.Cost) / SUM(REO2.Area)
                                        FROM RealEstateObject REO2
                                        WHERE REO2.DistrictCode = REO.DistrictCode)";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery12, connection))
                {
                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Адреса квартир со стоимостью 1 квадратного метра ниже средней по району:");

                        while (reader.Read())
                        {
                            string address12 = reader.GetString(0);

                            Console.WriteLine(address12);
                        }
                    }
                }
                Console.WriteLine();
                //2.13
                int currentYear = DateTime.Now.Year;

                // SQL-запрос для определения ФИО риэлторов, которые ничего не продали в текущем году
                string sqlQuery13 = @"SELECT DISTINCT R.LastName, R.FirstName, R.Patronymic
                                    FROM Realtor R
                                    WHERE R.RealtorCode NOT IN (
                                        SELECT DISTINCT S.RealtorCode
                                        FROM Sales S
                                        WHERE EXTRACT(YEAR FROM S.SaleDate) = @CurrentYear
                                    )";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery13, connection))
                {
                    command.Parameters.AddWithValue("@CurrentYear", currentYear);

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Риэлторы, которые ничего не продали в текущем году:");

                        while (reader.Read())
                        {
                            string lastName = reader.GetString(0);
                            string firstName = reader.GetString(1);
                            string middleName = reader.GetString(2);

                            Console.WriteLine($"{lastName} {firstName} {middleName}");
                        }
                    }
                }
                Console.WriteLine();
                //2.14
                int previousYear = currentYear - 1;
                string sqlQuery14 = @"SELECT 
                                        D.DistrictName AS ""1"",
                                        COUNT(CASE WHEN EXTRACT(YEAR FROM S.SaleDate) = 2023 THEN 1 END) AS ""2023"",
                                        COUNT(CASE WHEN EXTRACT(YEAR FROM S.SaleDate) = 2024 THEN 1 END) AS ""2024"",
                                        CASE 
                                            WHEN COUNT(CASE WHEN EXTRACT(YEAR FROM S.SaleDate) = 2023 THEN 1 END) = 0 THEN 100
                                            ELSE ((COUNT(CASE WHEN EXTRACT(YEAR FROM S.SaleDate) = 2024 THEN 1 END)::NUMERIC - COUNT(CASE WHEN EXTRACT(YEAR FROM S.SaleDate) = 2023 THEN 1 END)::NUMERIC) / COUNT(CASE WHEN EXTRACT(YEAR FROM S.SaleDate) = 2023 THEN 1 END) * 100)
                                        END AS ""Разница в %""
                                    FROM 
                                        Sales S
                                    JOIN 
                                        RealEstateObject REO ON S.ObjectCode = REO.ObjectCode
                                    JOIN 
                                        Districts D ON REO.DistrictCode = D.DistrictCode
                                    GROUP BY 
                                        D.DistrictName;";

                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery14, connection))
                {
                    command.Parameters.AddWithValue("@CurrentYear", currentYear);
                    command.Parameters.AddWithValue("@PreviousYear", previousYear);

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Название района\t2023\t2024\tРазница в %");

                        while (reader.Read())
                        {
                            string districtName14 = reader.GetString(0);
                            int previousYearSales = reader.GetInt32(1);
                            int currentYearSales = reader.GetInt32(2);
                            double percentChange = reader.GetDouble(3);

                            Console.WriteLine($"{districtName14}\t\t{previousYearSales}\t{currentYearSales}\t{percentChange}%");
                        }
                    }
                }
                Console.WriteLine();
                //2.15
                int objectCode = 1;
                string sqlQuery15 = @"SELECT
                                        C.CriteriaName AS ""Критерий"",
                                        AVG(EvaluationValue * 10) AS ""Средняя оценка"",
                                        CASE
                                            WHEN AVG(EvaluationValue * 10) >= 90 THEN 'превосходно'
                                            WHEN AVG(EvaluationValue * 10) >= 80 THEN 'очень хорошо'
                                            WHEN AVG(EvaluationValue * 10) >= 70 THEN 'хорошо'
                                            WHEN AVG(EvaluationValue * 10) >= 60 THEN 'удовлетворительно'
                                            ELSE 'неудовлетворительно'
                                        END AS ""Текст""
                                    FROM
                                        Evaluations ET
                                    JOIN
                                        EvaluationCriteria C ON ET.CriteriaCode = C.CriteriaCode
                                    WHERE
                                        ET.ObjectCode = @RealEstateObjectID
                                    GROUP BY
                                        C.CriteriaName";
                using (NpgsqlCommand command = new NpgsqlCommand(sqlQuery15, connection))
                {
                    command.Parameters.AddWithValue("@RealEstateObjectID", objectCode);

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        Console.WriteLine("Критерий\tСредняя оценка\t\tТекст");

                        while (reader.Read())
                        {
                            string criterion1 = reader.GetString(0);
                            double averageEvaluation = reader.GetDouble(1);
                            string text = reader.GetString(2);
                            string evaluationText;
                            if (text == "превосходно")
                            {
                                evaluationText = "5 из 5";
                            }
                            else if (text == "очень хорошо")
                            {
                                evaluationText = "4 из 5";
                            }
                            else if (text == "хорошо")
                            {
                                evaluationText = "3,5 из 5";
                            }
                            else if (text == "удовлетворительно")
                            {
                                evaluationText = "3 из 5";
                            }
                            else
                            {
                                evaluationText = "2 из 5";
                            }

                            Console.WriteLine($"{criterion1}\t{evaluationText}\t\t\t{text}");
                        }
                    }
                }
            }
        }
    }
}
