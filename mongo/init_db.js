db = db.getSiblingDB("patient_monitoring");

db.patients.deleteMany({});
db.devices.deleteMany({});
db.sensor_readings.deleteMany({});
db.alerts.deleteMany({});

db.patients.insertMany([
  {
    patientId: "P-0001",
    name: "Ali Ben Salah",
    dateOfBirth: "1985-06-15",
    gender: "M",
    ward: "Cardiology",
    medicalHistory: ["hypertension"],
    assignedDevices: ["DEV-HR-0001", "DEV-TEMP-0001", "DEV-OX-0001", "DEV-BP-0001", "DEV-RR-0001"]
  },
  {
    patientId: "P-0002",
    name: "Sara Trabelsi",
    dateOfBirth: "1992-09-22",
    gender: "F",
    ward: "Emergency",
    medicalHistory: ["asthma"],
    assignedDevices: ["DEV-HR-0002", "DEV-TEMP-0002", "DEV-OX-0002", "DEV-BP-0002", "DEV-RR-0002"]
  },
  {
    patientId: "P-0003",
    name: "Mohamed Jaziri",
    dateOfBirth: "1978-02-11",
    gender: "M",
    ward: "Intensive Care",
    medicalHistory: ["diabetes", "cardiac arrhythmia"],
    assignedDevices: ["DEV-HR-0003", "DEV-TEMP-0003", "DEV-OX-0003", "DEV-BP-0003", "DEV-RR-0003"]
  },
  {
    patientId: "P-0004",
    name: "Nour Hammami",
    dateOfBirth: "2001-12-05",
    gender: "F",
    ward: "General Medicine",
    medicalHistory: [],
    assignedDevices: ["DEV-HR-0004", "DEV-TEMP-0004", "DEV-OX-0004", "DEV-BP-0004", "DEV-RR-0004"]
  },
  {
    patientId: "P-0005",
    name: "Youssef Mansour",
    dateOfBirth: "1969-04-18",
    gender: "M",
    ward: "Cardiology",
    medicalHistory: ["hypertension", "high cholesterol"],
    assignedDevices: ["DEV-HR-0005", "DEV-TEMP-0005", "DEV-OX-0005", "DEV-BP-0005", "DEV-RR-0005"]
  },
  {
    patientId: "P-0006",
    name: "Meriem Saidi",
    dateOfBirth: "1988-07-30",
    gender: "F",
    ward: "Pulmonology",
    medicalHistory: ["chronic bronchitis"],
    assignedDevices: ["DEV-HR-0006", "DEV-TEMP-0006", "DEV-OX-0006", "DEV-BP-0006", "DEV-RR-0006"]
  },
  {
    patientId: "P-0007",
    name: "Karim Ayari",
    dateOfBirth: "1995-01-09",
    gender: "M",
    ward: "Emergency",
    medicalHistory: ["allergy"],
    assignedDevices: ["DEV-HR-0007", "DEV-TEMP-0007", "DEV-OX-0007", "DEV-BP-0007", "DEV-RR-0007"]
  },
  {
    patientId: "P-0008",
    name: "Ines Mabrouk",
    dateOfBirth: "1975-11-21",
    gender: "F",
    ward: "Intensive Care",
    medicalHistory: ["respiratory failure"],
    assignedDevices: ["DEV-HR-0008", "DEV-TEMP-0008", "DEV-OX-0008", "DEV-BP-0008", "DEV-RR-0008"]
  },
  {
    patientId: "P-0009",
    name: "Sami Khelifi",
    dateOfBirth: "1982-05-14",
    gender: "M",
    ward: "General Medicine",
    medicalHistory: ["diabetes"],
    assignedDevices: ["DEV-HR-0009", "DEV-TEMP-0009", "DEV-OX-0009", "DEV-BP-0009", "DEV-RR-0009"]
  },
  {
    patientId: "P-0010",
    name: "Leila Gharbi",
    dateOfBirth: "1999-03-27",
    gender: "F",
    ward: "Pulmonology",
    medicalHistory: ["asthma"],
    assignedDevices: ["DEV-HR-0010", "DEV-TEMP-0010", "DEV-OX-0010", "DEV-BP-0010", "DEV-RR-0010"]
  }
]);

const devices = [];

for (let i = 1; i <= 10; i++) {
  const suffix = String(i).padStart(4, "0");
  const patientId = `P-${suffix}`;

  devices.push(
    {
      deviceId: `DEV-HR-${suffix}`,
      type: "HeartRateMonitor",
      manufacturer: "Philips",
      capabilities: ["HeartRate"],
      patientId: patientId,
      installedAt: new Date("2026-01-10T08:00:00Z")
    },
    {
      deviceId: `DEV-TEMP-${suffix}`,
      type: "TemperatureSensor",
      manufacturer: "Medtronic",
      capabilities: ["Temperature"],
      patientId: patientId,
      installedAt: new Date("2026-01-10T08:00:00Z")
    },
    {
      deviceId: `DEV-OX-${suffix}`,
      type: "OxygenSensor",
      manufacturer: "GE Healthcare",
      capabilities: ["OxygenLevel"],
      patientId: patientId,
      installedAt: new Date("2026-01-10T08:00:00Z")
    },
    {
      deviceId: `DEV-BP-${suffix}`,
      type: "BloodPressureMonitor",
      manufacturer: "Omron",
      capabilities: ["BloodPressure"],
      patientId: patientId,
      installedAt: new Date("2026-01-10T08:00:00Z")
    },
    {
      deviceId: `DEV-RR-${suffix}`,
      type: "RespiratoryRateSensor",
      manufacturer: "Mindray",
      capabilities: ["RespiratoryRate"],
      patientId: patientId,
      installedAt: new Date("2026-01-10T08:00:00Z")
    }
  );
}

db.devices.insertMany(devices);

db.sensor_readings.createIndex({ patientId: 1, timestamp: -1 });
db.sensor_readings.createIndex({ metric: 1 });
db.sensor_readings.createIndex({ alertLevel: 1 });
db.alerts.createIndex({ patientId: 1, timestamp: -1 });
db.alerts.createIndex({ alertLevel: 1 });

print("Database patient_monitoring initialized successfully.");
print("Inserted 10 patients.");
print("Inserted 50 devices.");
print("Created indexes for sensor_readings and alerts.");