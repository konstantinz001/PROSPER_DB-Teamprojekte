using ClassLibrary.Model;
using ClassLibrary.Processor;
using Microsoft.AspNetCore.Mvc;


namespace PROSPER.API.Controllers
{
    /// <summary>
    /// 
    /// Controller und private Methoden zum Bereitstellen der Funktionalitäten der API
    /// 
    /// </summary>

    [Route("api/[controller]")]
    [ApiController]
    public class PROSPERController : ControllerBase
    {
        ISqlExecutor _db;

        public PROSPERController(ISqlExecutor db)
        {
            _db = db;
        }



        [HttpGet]
        [Route("")]
        public IActionResult Index(int? releaseStart, int? releaseEnd, string? sachgruppe, string? suchBegriff_1, string? suchBegriff_2, string? suchBegriff_3)
        {
            if (releaseStart.HasValue && releaseEnd.HasValue && !string.IsNullOrEmpty(sachgruppe))
            {
                var data = getData(releaseStart.Value, releaseEnd.Value, sachgruppe, suchBegriff_1, suchBegriff_2, suchBegriff_3);
                return Ok(data);
            }

            else if (releaseStart.HasValue && releaseEnd.HasValue)
            {
                var data = getData(releaseStart.Value, releaseEnd.Value, suchBegriff_1, suchBegriff_2, suchBegriff_3);
                return Ok(data);
            }

            else
            {
                string syntax = "SYNTAX:\n\n" +
                    "- URL?releaseStart={int}&releaseEnd={int}\n" +
                    "- URL?releaseStart={int}&releaseEnd={int}&sachgruppe={string}\n" +
                    "- URL?releaseStart={int}&releaseEnd={int}&sachgruppe={string}&suchBegriff_1={string}\n" +
                    "- URL?releaseStart={int}&releaseEnd={int}&sachgruppe={string}&suchBegriff_1={string}&suchBegriff_2={string}\n" +
                    "- URL?releaseStart={int}&releaseEnd={int}&sachgruppe={string}&suchBegriff_1={string}&suchBegriff_2={string}&suchBegriff_3={string}\n" +
                    "- URL/Fakultaeten";
                return Ok(syntax);
            }
        }

        [HttpGet]
        [Route("Fakultaeten")]
        public IActionResult Fakultaeten()
        {
            return Ok(_db.GetFakultäten());
        }


        private List<DNB_Dataset> getData(int releaseStart, int releaseEnd, string sachgruppenBezeichnung, string? suchBegriff_1, string? sucheBegriff_2, string? suchBegriff_3)
        {
            var data = new List<DNB_Dataset>();

            for (int year = (int)releaseStart; year <= releaseEnd; year++)
            {

                var tmp = _db.GetDNB_Dataset(year, sachgruppenBezeichnung);
                data = data.Concat(tmp).ToList();
            }

            if (data.Count > 0)
            {
                data = OnSearch(data, suchBegriff_1, sucheBegriff_2, suchBegriff_3);
            }
            return data;
        }


        private List<DNB_Dataset> getData(int releaseStart, int releaseEnd, string? suchBegriff_1, string? sucheBegriff_2, string? suchBegriff_3)
        {
            var data = new List<DNB_Dataset>();

            for (int year = (int)releaseStart; year <= releaseEnd; year++)
            {

                var tmp = _db.GetDNB_Dataset(year);
                data = data.Concat(tmp).ToList();
            }

            if (data.Count > 0)
            {
                data = OnSearch(data, suchBegriff_1, sucheBegriff_2, suchBegriff_3);
            }
            return data;
        }


        private List<DNB_Dataset> OnSearch(List<DNB_Dataset> data, string? volltextSuchString1, string? volltextSuchString2, string? volltextSuchString3)
        {
            if (!string.IsNullOrEmpty(volltextSuchString1) && !string.IsNullOrEmpty(volltextSuchString2) && !string.IsNullOrEmpty(volltextSuchString3))
            {
                return data
                .Where(x =>
                    x.TITLE!.Contains(" " + volltextSuchString1 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.StartsWith(volltextSuchString1 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.EndsWith(" " + volltextSuchString1, StringComparison.CurrentCultureIgnoreCase) ||
                     x.TITLE!.Contains(volltextSuchString1) ||
                    x.CREATOR!.Contains(volltextSuchString1))
                .Where(x =>
                    x.TITLE!.Contains(" " + volltextSuchString2 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.StartsWith(volltextSuchString2 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.EndsWith(" " + volltextSuchString2, StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.Contains(volltextSuchString2) ||
                    x.CREATOR!.Contains(volltextSuchString2))
                 .Where(x =>
                    x.TITLE!.Contains(" " + volltextSuchString3 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.StartsWith(volltextSuchString3 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.EndsWith(" " + volltextSuchString3, StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.Contains(volltextSuchString3) ||
                    x.CREATOR!.Contains(volltextSuchString3)).ToList();
            }
            else if (!string.IsNullOrEmpty(volltextSuchString1) && !string.IsNullOrEmpty(volltextSuchString2))
            {
                return data
                .Where(x => (
                    x.TITLE!.Contains(" " + volltextSuchString1 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.StartsWith(volltextSuchString1 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.EndsWith(" " + volltextSuchString1, StringComparison.CurrentCultureIgnoreCase)) ||
                    x.TITLE!.Contains(volltextSuchString1) ||
                    x.CREATOR!.Contains(volltextSuchString1))
                .Where(x => (
                    x.TITLE!.Contains(" " + volltextSuchString2 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.StartsWith(volltextSuchString2 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.EndsWith(" " + volltextSuchString2, StringComparison.CurrentCultureIgnoreCase)) ||
                    x.TITLE!.Contains(volltextSuchString2) ||
                    x.CREATOR!.Contains(volltextSuchString2)).ToList();
            }
            else if (!string.IsNullOrEmpty(volltextSuchString2) && !string.IsNullOrEmpty(volltextSuchString3))
            {
                return data
                .Where(x => (
                    x.TITLE!.Contains(" " + volltextSuchString2 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.StartsWith(volltextSuchString2 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.EndsWith(" " + volltextSuchString2, StringComparison.CurrentCultureIgnoreCase)) ||
                    x.TITLE!.Contains(volltextSuchString2) ||
                    x.CREATOR!.Contains(volltextSuchString2))
                .Where(x => (
                    x.TITLE!.Contains(" " + volltextSuchString3 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.StartsWith(volltextSuchString3 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                    x.TITLE!.EndsWith(" " + volltextSuchString3, StringComparison.CurrentCultureIgnoreCase)) ||
                    x.TITLE!.Contains(volltextSuchString3) ||
                    x.CREATOR!.Contains(volltextSuchString3))
                .ToList();
            }
            else if (!string.IsNullOrEmpty(volltextSuchString1))
            {
                return data
               .Where(x => (
                   x.TITLE!.Contains(" " + volltextSuchString1 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                   x.TITLE!.StartsWith(volltextSuchString1 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                   x.TITLE!.EndsWith(" " + volltextSuchString1, StringComparison.CurrentCultureIgnoreCase)) ||
                   x.TITLE!.Contains(volltextSuchString1) ||
                   x.CREATOR!.Contains(volltextSuchString1))
               .ToList();
            }
            else if (!string.IsNullOrEmpty(volltextSuchString2))
            {
                return data
                .Where(x => (
                  x.TITLE!.Contains(" " + volltextSuchString2 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                  x.TITLE!.StartsWith(volltextSuchString2 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                  x.TITLE!.EndsWith(" " + volltextSuchString2, StringComparison.CurrentCultureIgnoreCase)) ||
                  x.TITLE!.Contains(volltextSuchString2) ||
                  x.CREATOR!.Contains(volltextSuchString2))
              .ToList();
            }
            else if (!string.IsNullOrEmpty(volltextSuchString3))
            {
                return data
                .Where(x => (
                  x.TITLE!.Contains(" " + volltextSuchString3 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                  x.TITLE!.StartsWith(volltextSuchString3 + " ", StringComparison.CurrentCultureIgnoreCase) ||
                  x.TITLE!.EndsWith(" " + volltextSuchString3, StringComparison.CurrentCultureIgnoreCase)) ||
                  x.TITLE!.Contains(volltextSuchString3) ||
                  x.CREATOR!.Contains(volltextSuchString3))
              .ToList();
            }
            else
            {
                return data;
            }
        }
    }
}
