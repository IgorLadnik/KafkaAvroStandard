using System;
using System.IO;
using System.Text;
using System.Net;
using System.Net.Sockets;

namespace HttpClientLib
{
    public class HttpClient
    {
        #region Vars

        private readonly bool _isHttpS = false;
        private readonly string _strUser = string.Empty;
        private readonly string _strPassword = string.Empty;
        private string strContentType = string.Empty;   // shows type of returned byte array
        private HttpWebRequest request = null;  // for async. mode

        #endregion // Vars

        #region Ctor 	

        public HttpClient(bool isHttpS = false, string strUser = "", string strPassword = "", int binaryMode = 0)
        {
            _isHttpS = isHttpS;
            _strUser = strUser;
            _strPassword = strPassword;
            BinaryMode = binaryMode;
        }

        #endregion // Ctor 

        #region Properties

        public int BinaryMode { get; set; }

        #endregion // Properties

        #region Public Methods

        // Interface method to POST string strData to strUrl. Calls appropriate Post() method. 
        public byte[] HttpConnectEx(string strServer, string strCommand, int port, string strData)
        {
            var strUrl = ConstructUrl(strServer, strCommand, port);
            var strPostData = strData; //HttpUtility.UrlEncode(strData);
            var btOut = Post(strUrl, strPostData);
            return PostprocessData(btOut);
        }

        // Interface method to POST byte[] btData to strUrl.  Calls appropriate Post() method.
        public byte[] HttpConnectEx(string strServer, string strCommand, int port, byte[] btData)
        {
            var strUrl = ConstructUrl(strServer, strCommand, port);
            var btActualData = PreprocessData(btData);
            var btOut = Post(strUrl, btActualData);
            return PostprocessData(btOut);
        }

        // Converts string to byte[] and calls another Post() method. 
        public byte[] Post(string strUrl, string strPostData)
        {
            //var btPostData = Encoding.GetEncoding(1252).GetBytes(strPostData);
            var btPostData = Encoding.UTF8.GetBytes(strPostData);
            return Post(strUrl, btPostData);
        }

        // Actually POSTs byte[] btPostData to strUrl. 
        public byte[] Post(string strUrl, byte[] btPostData)
        {
            var request = PreparePostRequest(strUrl, btPostData);
            var response = (HttpWebResponse)request.GetResponse();
            return GetBytesResponse(response);
        }

        // Obtain bytes from response stream.
        public byte[] GetBytesResponse(HttpWebResponse response)
        {
            var smResponse = response.GetResponseStream();

            var chunkLen = 1024 * 5;
            var btResponse = new byte[chunkLen];
            var outStream = new MemoryStream();
            var read = 0;
            while (true)
            {
                read = smResponse.Read(btResponse, 0, chunkLen);
                if (read > 0)
                    outStream.Write(btResponse, 0, read);
                else
                    break;
            }

            strContentType = response.ContentType;
            response.Close();

            return outStream.ToArray();
        }

        // Actually GETs to the strUrl. 
        public byte[] Get(string strUrl, int timeoutInSec)
        {
            // Create Web Request 
            var request = (HttpWebRequest)WebRequest.Create(strUrl);

            // Set Credentials
            request.Credentials = new NetworkCredential(_strUser, _strPassword);

            // Set Timeout
            request.Timeout = 1000 * timeoutInSec;

            // Response
            var response = (HttpWebResponse)request.GetResponse();
            return GetBytesResponse(response);
        }

        // Test whether server:port connected
        static public bool IsConnected(string server, int port, out string strErrMessage)
        {
            //			IPHostEntry heserver = Dns.Resolve(server);
            //			IPAddress ipAddr = heserver.AddressList[0];
            return HttpClient.IsConnected(IPAddress.Parse(server), port, out strErrMessage);
        }

        // Test whether ipAddr:port connected
        static public bool IsConnected(IPAddress ipAddr, int port, out string strErrMessage)
        {
            var br = false;
            strErrMessage = null;
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var iep = new IPEndPoint(ipAddr, port);
            try
            {
                socket.Connect(iep);
                br = socket.Connected;
            }
            catch (Exception e)
            {
                strErrMessage = e.Message;
            }

            return br;
        }

        #region Public Methods, for Asynchronous Mode

        // Interface method to POST string strData to strUrl. Calls appropriate Post() method. 
        public IAsyncResult HttpConnectEx(string strServer, string strCommand, int port, string strData, AsyncCallback onPostEnd)
        {
            var strUrl = ConstructUrl(strServer, strCommand, port);
            var strPostData = strData; // HttpUtility.UrlEncode(strData);
            return Post(strUrl, strPostData, onPostEnd);
        }

        // Interface method to POST byte[] btData to strUrl.  Calls appropriate Post() method.
        public IAsyncResult HttpConnectEx(string strServer, string strCommand, int port, byte[] btData,
            AsyncCallback onPostEnd)
        {
            var strUrl = ConstructUrl(strServer, strCommand, port);
            var btActualData = PreprocessData(btData);
            return Post(strUrl, btActualData, onPostEnd);
        }

        // Convert string to byte[] and call another Post() method. 
        public IAsyncResult Post(string strUrl, string strPostData, AsyncCallback onPostEnd)
        {
            //var btPostData = Encoding.GetEncoding(1252).GetBytes(strPostData);
            var btPostData = Encoding.UTF8.GetBytes(strPostData);
            return Post(strUrl, btPostData, onPostEnd);
        }

        // Actually POST byte[] btPostData to strUrl asynchronously. 
        public IAsyncResult Post(string strUrl, byte[] btPostData, AsyncCallback onPostEnd)
        {
            var request = PreparePostRequest(strUrl, btPostData);
            this.request = request;
            return request.BeginGetResponse(onPostEnd, this);
        }

        #endregion // Public Methods, for Asynchronous Mode

        #endregion // Public Methods

        #region Private Methods

        private HttpWebRequest PreparePostRequest(string strUrl, byte[] btPostData)
        {
            // Create Web Request 
            var request = (HttpWebRequest)WebRequest.Create(strUrl);

            // Set Credentials
            request.Credentials = new NetworkCredential(_strUser, _strPassword);

            // POST
            request.Method = "POST";

            // Set Timeout
            request.Timeout = 30000;     // 30 sec

            if (null != btPostData && btPostData.Length > 0)
            {
                request.ContentLength = btPostData.Length;
                request.ContentType = "application/x-www-form-urlencoded";
                request.GetRequestStream().Write(btPostData, 0, btPostData.Length);
                request.GetRequestStream().Close(); //??
            }
            else
                request.ContentLength = 0;
            //??
            //			if (null != btPostData && btPostData.Length > 0)
            //			{
            //				request.ContentLength = btPostData.Length;
            //				Stream smPostData = request.GetRequestStream();
            //				smPostData.Write(btPostData, 0, btPostData.Length);
            //				smPostData.Close();
            //			}
            //			else
            //				request.ContentLength = 0;

            return request;
        }

        private string ConstructUrl(string strServer, string strCommand, int port)
        {
            var strPort = (0 >= port) ? string.Empty : ":" + port.ToString();
            var strUrl = _isHttpS ? "https://" : "http://";
            StringBuilder sb = new StringBuilder(strUrl);
            sb.Append(strServer);
            sb.Append(strPort);
            sb.Append(strCommand);
            return sb.ToString();
        }

        private byte[] PreprocessData(byte[] btData)
        {
            byte[] btActualData;

            if ((BinaryMode & 0x01) != 0)
            {
                // Binary mode: add prefix '@'
                btActualData = new byte[btData.Length + 2];
                btActualData[0] = (byte)'@';
                btActualData[1] = (byte)0;
                btData.CopyTo(btActualData, 2);
            }
            else
                // Text mode: data are sent as is
                btActualData = btData;

            return btActualData;
        }

        private byte[] PostprocessData(byte[] btOut)
        {
            if ((BinaryMode & 0x02) == 0 && (byte)0 != btOut[0])
            {
                // Text result
            }
            else
                // Binary result: skip leading 0
                if ((byte)0 == btOut[0])
                return SkipLeadingBytes(btOut, 1);

            return btOut;
        }

        // Static 

        static private byte[] SkipLeadingBytes(byte[] btInp, int iNumBytes)
        {
            var btOut = new byte[btInp.Length - iNumBytes];
            var msmTemp = new MemoryStream(btInp);
            msmTemp.Position = iNumBytes;
            msmTemp.Read(btOut, 0, btOut.Length);
            return btOut;
        }

        #endregion // Private Methods       
    }
}


