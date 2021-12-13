import datetime as dt
import fnmatch
import os
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader


class PageBuilderWriter:

    def __init__(self, dir_out_html: Path):
        self.TEMPLATE_ENVIRONMENT = self._get_templates()
        self.dir_out_html = dir_out_html

    def _get_templates(self):
        """Get path to HTML templates"""
        PATH = os.path.dirname(os.path.abspath(__file__))
        TEMPLATE_ENVIRONMENT = Environment(
            autoescape=False,
            loader=FileSystemLoader(os.path.join(PATH, '../html_pagebuilder/templates')),
            trim_blocks=False)
        return TEMPLATE_ENVIRONMENT

    def write(self, outfile, template, context):
        """Write HTML page"""
        with open(outfile, 'w') as f:
            html = self.TEMPLATE_ENVIRONMENT.get_template(template).render(context)
            f.write(html)


def get_buildtime():
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    nowtime = dt.datetime.now()
    buildtime = '{:%d %b %Y %H:%M:%S}'.format(nowtime)
    return buildtime


class PageBuilderSiteIndex(PageBuilderWriter):
    """Build HTML page for site HTML pages"""

    def __init__(self, site, template):
        super().__init__()
        self.site = site
        self.template = template

        # Environment
        self.outfile = self.outdir / site / "index.html"
        self.buildtime = get_buildtime()

    def build(self):
        self.logger.info("Building HTML site index pages ...")
        self.links_to_measurement_pages = self.search_html_pages()
        self.context = self.set_context()
        self.write(outfile=self.outfile, template=self.template, context=self.context)

    def search_html_pages(self):
        links_to_measurement_pages = []
        for file in os.listdir(self.outfile.parent):
            if fnmatch.fnmatch(file, '*.html') & (file != 'index.html'):
                links_to_measurement_pages.append(file)
        links_to_measurement_pages.sort()
        return links_to_measurement_pages

    def set_context(self):
        context = {'links_to_measurement_pages': self.links_to_measurement_pages,
                   'site': self.site,
                   'buildtime': self.buildtime}
        return context


class PageBuilderMeasurements(PageBuilderWriter):
    """Build HTML page for measurements"""

    class_id = "[HTML_PAGEBUILDER]"

    def __init__(self, site, filegroup, template, filescanner_df, varscanner_df, dir_out_html, run_id: str, logger):
        super().__init__(dir_out_html)
        self.site = site
        self.filegroup = filegroup
        self.template = template
        self.filescanner_df = filescanner_df
        self.varscanner_df = varscanner_df
        self.dir_out_html = dir_out_html
        self.run_id = run_id
        self.logger=logger

        # Environment
        self.outfile = self.dir_out_html / f'{filegroup}_{self.run_id}.html'
        self.buildtime = get_buildtime()

        # Init new vars
        self.stats_numfoundfiles = None
        self.stats_unique_filetypes = {}
        self.newest_files = []
        self.oldest_files = []

    def build(self):
        self.logger.info(f"{self.class_id} Building HTML measurement pages ...")
        self.stats()
        self.context = self.set_context()
        self.write(outfile=self.outfile, template=self.template, context=self.context)

    def stats(self):
        # Detect unique filetypes and their number of occurrences
        stats_unique_filetypes = list(set(self.filescanner_df['config_filetype'].to_list()))
        for u in stats_unique_filetypes:
            self.stats_unique_filetypes[u] = {}
            self.stats_unique_filetypes[u]['numfiles'] = len(
                self.filescanner_df.loc[self.filescanner_df['config_filetype'] == u, :])
            self.stats_unique_filetypes[u]['filesize_median'] = self.filescanner_df.loc[
                self.filescanner_df['config_filetype'] == u, 'filesize'].median()
            self.stats_unique_filetypes[u]['filesize_max'] = self.filescanner_df.loc[
                self.filescanner_df['config_filetype'] == u, 'filesize'].max()
            self.stats_unique_filetypes[u]['filesize_min'] = self.filescanner_df.loc[
                self.filescanner_df['config_filetype'] == u, 'filesize'].min()

        # Total number of files
        self.stats_numfoundfiles = len(self.filescanner_df)

        # Newest files
        _df = self.filescanner_df.sort_values(by='filemtime', axis=0, ascending=False).head(5)
        self.newest_files = _df['filename'].to_list()

        # Oldest files
        _df = self.filescanner_df.sort_values(by='filemtime', axis=0, ascending=True).head(5)
        self.oldest_files = _df['filename'].to_list()

    def set_context(self):
        context = {'table_filescanner': self.filescanner_df.to_html(classes='js-sort-table'),
                   'site': self.site,
                   'filegroup': self.filegroup,
                   'unique_filetypes': self.stats_unique_filetypes,
                   'newest_files': self.newest_files,
                   'oldest_files': self.oldest_files,
                   'numfoundfiles': self.stats_numfoundfiles,
                   'buildtime': self.buildtime}

        if isinstance(self.varscanner_df, pd.DataFrame):
            context['table_varscanner'] = self.varscanner_df.to_html(classes='js-sort-table')
        else:
            context['table_varscanner'] = None
        return context
