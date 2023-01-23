import os
import sys
import hashlib
import urllib2
import yaml
import argparse
import shutil
from re import sub

def get_remote_sha256_sum(url, max_file_size=1024*1024*1024):
    remote = urllib2.urlopen(url)
    hash = hashlib.sha256()
    total_read = 0
    while True:
        data = remote.read(4096)
        total_read += 4096
        if not data or total_read > max_file_size:
            break
        hash.update(data)
    return hash.hexdigest()


def read_vars(var_file):
    """
    Read the variables file
    """
    try:
        with open(var_file, "r") as f:
            kargovars = yaml.load(f)
    except:
        print(
            "Can't read variables file %s" % var_file
        )
        sys.exit(1)
    return kargovars


def get_kube_sha256(version, download_url, binaries):
    kube_sha256 = dict()
    for k in binaries:
        s = get_remote_sha256_sum(download_url + '/' + k)
        kube_sha256[k] = s
    kube_sha256['kube_apiserver'] = kube_sha256.pop('kube-apiserver')
    return(kube_sha256)


def file_sub(file, regex, string):
    "Substitute string in a file"
    shutil.move(file, file + '~')
    f = open(file + '~', 'r')
    data = f.read()
    o = open(file, 'w')
    o.write(sub(regex, string, data))
    f.close()
    o.close()
    os.remove(file + '~')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='change_k8s_version',
        description='%(prog)s changes the version to be installed with kargo',
    )

    parser.add_argument(
        '-v', '--version', dest='kube_version', required=True,
        help="kubernetes version"
    )
    parser.add_argument(
        '-r', '--repository', dest='docker_repository', required=True,
        help="hyperkube docker repository"
    )
    args = parser.parse_args()

    kargo_root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    file_sub(
        os.path.join(kargo_root_path, 'roles/kubernetes/node/defaults/main.yml'),
        r'.*hyperkube_image_repo.*', 'hyperkube_image_repo: "%s"' % args.docker_repository
    )
    file_sub(
        os.path.join(kargo_root_path, 'roles/kubernetes/node/defaults/main.yml'),
        r'.*hyperkube_image_tag.*', 'hyperkube_image_tag: "%s"' % args.kube_version
    )

    kube_binaries = ['kubelet', 'kubectl', 'kube-apiserver']
    var_files = [
        os.path.join(kargo_root_path, 'roles/uploads/vars/kube_versions.yml'),
        os.path.join(kargo_root_path, 'roles/download/vars/kube_versions.yml')
    ]
    kube_download_url = "https://storage.googleapis.com/kubernetes-release/release/%s/bin/linux/amd64" % args.kube_version

    new = get_kube_sha256(args.kube_version, kube_download_url, kube_binaries)
    for f in var_files:
        current = read_vars(f)
        current['kube_checksum'][args.kube_version] = new
        current['kube_version'] = args.kube_version
        with open(f, 'w') as out:
            out.write(yaml.dump(current, indent=4, default_flow_style=False))
            
           
  def _parse_args(self):
    parser = argparse.ArgumentParser(
        description='Summarize a file by line. Clusters similar lines '\
          'using a distance metric.')

    parser.add_argument('input', type=str, default='-',
      help='Input file, use "-" for stdin')
    parser.add_argument('-m', '--metric', default='hash',
      help='Metric to use, one of ' + ', '.join(
        [x.NAME for x in Distance.__subclasses__()]))
    parser.add_argument('-d', '--distance', type=float, default=5,
      help='Distance metric to use')
    parser.add_argument('-e', '--equiv', action='append', default=[],
      help='List of regular expressions to render "equivalent"')
    parser.add_argument('-n', '--not-equiv', action='append', default=[],
      help='List of regular expressions to blacklist from --equiv')
    parser.add_argument('-v', action='count',
      help='Verbose output, specify twice for DEBUG level')
    parser.add_argument('--print-all', action='store_true',
      help='Print all of the lines matches in addition to the clusters')

    self._args = parser.parse_args()

    return parser.parse_args()

  def _parse(self):
    if self._args.input == '-':
      in_file = sys.stdin
    else:
      in_file = open(self._args.input, 'r')

    distance = None
    for subclass in Distance.__subclasses__():
      if subclass.NAME == self._args.metric:
        distance = subclass()
    if not distance:
      raise RuntimeError('invalid metric {}'.format(self._args.metric))

    equiv_classes = EquivClasses(self._args.equiv, self._args.not_equiv)

    entries = []
    clusters = {}
    line_num = 1

    for line in in_file:
      line = line.strip()
      words = line.split()
      words = [equiv_classes.replace(x) for x in words]
      _log.debug('words {}'.format(words))
      
      found = False
      for cluster_name, cluster in clusters.items():
        if distance.measure(cluster.center, words) <= self._args.distance:
          _log.debug('clustering line {} into cluster {}'.format(
            line_num, cluster_name))
          entry = Entry(line_num, cluster_name, line, words)
          entries.append(entry)
          cluster.entries.append(entry)
          found = True

      if not found:
        cluster_name = 'cluster_{}'.format(len(clusters))
        cluster = Cluster(words)
        entry = Entry(line_num, cluster_name, line, words)
        entries.append(entry)
        cluster.entries.append(entry)

        clusters[cluster_name] = cluster
        _log.debug('line {} is a new cluster {}'.format(
          line_num, cluster_name))
      line_num += 1

    for _, cluster_name in sorted(
        [(-len(clusters[n].entries), n) for n in clusters]):
      cluster = clusters[cluster_name]
      print('{}\t{}\t{}'.format(
        cluster_name,
        len(cluster.entries), 
        ' '.join(cluster.center)))
      if self._args.print_all:
        for entry in cluster.entries:
          print('{}\t|\t{}'.format(cluster_name, entry.line))
