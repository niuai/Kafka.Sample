using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Sample.Models
{
    public class TestEventData
    {
        public int Id { get; set; }

        public string Message { get; set; }

        public DateTime EventTime { get; set; } = DateTime.Now;

        public string TopicName { get; set; }
    }
}
