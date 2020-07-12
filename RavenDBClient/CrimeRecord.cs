using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RavenDBClient
{
    public class CrimeRecord
    {
        public string ID { get; set; }

        public string CaseNumber { get; set; }

        public DateTime Date { get; set; }

        public string Block { get; set; }

        public string IUCR { get; set; }

        public string PrimaryType { get; set; }

        public string Description { get; set; }

        public string LocationDescription { get; set; }

        public bool Arrest { get; set; }

        public bool Domestic { get; set; }

        public string Beat { get; set; }

        public string District { get; set; }

        public string Ward { get; set; }

        public string CommunityArea { get; set; }

        public string FBICode { get; set; }
        
        public double XCoordinate { get; set; }

        public double YCoordinate { get; set; }

        public int Year { get; set; }

        public DateTime UpdatedOn { get; set; }

        public double Latitude { get; set; }

        public double Longitude { get; set; }

        public string Location { get; set; }
    }
}
