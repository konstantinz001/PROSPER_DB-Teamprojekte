using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace DNB_Frontend.Pages
{
    public class DownloadModel : PageModel
    {
        private readonly IWebHostEnvironment _env;

        public DownloadModel (IWebHostEnvironment env)
        {
            _env = env; 
        }

        public IActionResult OnGet(string id)
        {
            string[] files = Directory.GetFiles(Path.Combine(_env.WebRootPath, "Data"));

            foreach(string file in files)
            {
                FileInfo fi = new FileInfo(file);
                if (fi.LastAccessTime < DateTime.Now.AddDays(-1))
                    fi.Delete();
            }

            var filePath = Path.Combine(_env.WebRootPath, "Data", $"Archive_{id}.zip");
            byte[] fileBytes = System.IO.File.ReadAllBytes(filePath);
            System.IO.File.Delete(filePath);

            return File(fileBytes, "application/force-download", "Prosper.zip");


        }
    }
}
