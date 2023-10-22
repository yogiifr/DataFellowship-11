function transform(line) {
    var values = line.split(',');
    var obj = new Object();
    
    obj.customerID = values[0];
    obj.gender = values[1];
    obj.SeniorCitizen = values[2];
    obj.Partner = (values[3].toLowerCase() === "yes") ? true : false;
    obj.Dependents = (values[4].toLowerCase() === "yes") ? true : false;;
    obj.tenure = values[5];
    obj.PhoneService = (values[6].toLowerCase() === "yes") ? true : false;
    obj.MultipleLines = values[7];
    obj.InternetService = values[8];
    obj.OnlineSecurity = values[9];
    obj.OnlineBackup = values[10];
    obj.DeviceProtection = values[11];
    obj.TechSupport = values[12];
    obj.StreamingTV = values[13];
    obj.StreamingMovies = values[14];
    obj.Contract = values[15];
    obj.PaperlessBilling = (values[16].toLowerCase() === "yes") ? true : false;
    obj.PaymentMethod = values[17];
    obj.MonthlyCharges = values[18];
    obj.TotalCharges = values[19];
    obj.Churn = (values[20].toLowerCase() === "yes") ? true : false;
    
    var jsonString = JSON.stringify(obj);
    return jsonString;
}